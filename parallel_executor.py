"""
Parallel SQL Executor - Execute SQL batches in parallel with threading
"""
import pyodbc
import threading
import queue
import time
from typing import List, Dict, Callable, Optional
from dataclasses import dataclass
from datetime import datetime
import logging


@dataclass
class ExecutionResult:
    """Result of a batch execution"""
    batch_number: int
    success: bool
    rows_affected: int
    duration_seconds: float
    error_message: str = ""
    timestamp: str = ""


class ParallelExecutor:
    """Execute SQL batches in parallel using threading"""
    
    def __init__(self, connection_string: str, num_threads: int = 4):
        """
        Initialize parallel executor
        
        Args:
            connection_string: Database connection string
            num_threads: Number of parallel threads to use
        """
        self.connection_string = connection_string
        self.num_threads = num_threads
        self.results: List[ExecutionResult] = []
        self.stop_flag = threading.Event()
        self.error_occurred = threading.Event()
        self.progress_callback: Optional[Callable] = None
        self.lock = threading.Lock()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def set_progress_callback(self, callback: Callable[[int, int, str], None]):
        """
        Set callback function for progress updates
        
        Args:
            callback: Function(completed, total, message) -> None
        """
        self.progress_callback = callback
        
    def _report_progress(self, completed: int, total: int, message: str):
        """Report progress to callback if set"""
        if self.progress_callback:
            try:
                self.progress_callback(completed, total, message)
            except Exception as e:
                self.logger.error(f"Error in progress callback: {e}")
    
    def _worker(self, batch_queue: queue.Queue, total_batches: int):
        """Worker thread to execute SQL batches"""
        # Each thread gets its own connection
        connection = None
        
        try:
            connection = pyodbc.connect(self.connection_string)
            connection.autocommit = False  # Use transactions
            cursor = connection.cursor()
            
            while not self.stop_flag.is_set():
                try:
                    # Get batch from queue with timeout
                    batch = batch_queue.get(timeout=0.5)
                    if batch is None:  # Poison pill
                        break
                        
                    batch_number = batch['batch_number']
                    sql = batch['sql']
                    
                    # Check if we should stop due to earlier error
                    if self.error_occurred.is_set():
                        self.logger.info(f"Skipping batch {batch_number} due to earlier error")
                        batch_queue.task_done()
                        continue
                    
                    self.logger.info(f"Executing batch {batch_number}")
                    start_time = time.time()
                    
                    try:
                        # Execute the batch
                        cursor.execute(sql)
                        rows_affected = cursor.rowcount
                        
                        # Commit transaction
                        connection.commit()
                        
                        duration = time.time() - start_time
                        
                        result = ExecutionResult(
                            batch_number=batch_number,
                            success=True,
                            rows_affected=rows_affected,
                            duration_seconds=duration,
                            timestamp=datetime.now().isoformat()
                        )
                        
                        with self.lock:
                            self.results.append(result)
                            completed = len([r for r in self.results if r.success or r.error_message])
                            
                        self._report_progress(
                            completed,
                            total_batches,
                            f"Completed batch {batch_number} ({rows_affected} rows, {duration:.2f}s)"
                        )
                        
                        self.logger.info(f"Batch {batch_number} completed successfully")
                        
                    except Exception as e:
                        # Error occurred - rollback and signal all threads to stop
                        try:
                            connection.rollback()
                        except:
                            pass
                            
                        duration = time.time() - start_time
                        error_msg = str(e)
                        
                        result = ExecutionResult(
                            batch_number=batch_number,
                            success=False,
                            rows_affected=0,
                            duration_seconds=duration,
                            error_message=error_msg,
                            timestamp=datetime.now().isoformat()
                        )
                        
                        with self.lock:
                            self.results.append(result)
                            
                        self.logger.error(f"Batch {batch_number} failed: {error_msg}")
                        
                        # Signal error and stop all threads
                        self.error_occurred.set()
                        self.stop_flag.set()
                        
                        self._report_progress(
                            len(self.results),
                            total_batches,
                            f"ERROR in batch {batch_number}: {error_msg}"
                        )
                        
                    batch_queue.task_done()
                    
                except queue.Empty:
                    continue
                    
        except Exception as e:
            self.logger.error(f"Worker thread error: {e}")
            self.error_occurred.set()
            self.stop_flag.set()
            
        finally:
            if connection:
                try:
                    connection.close()
                except:
                    pass
    
    def execute_batches(self, batches: List[dict]) -> List[ExecutionResult]:
        """
        Execute SQL batches in parallel
        
        Args:
            batches: List of batch dictionaries from SQLParser
            
        Returns:
            List of ExecutionResult objects
        """
        if not batches:
            return []
        
        # Reset state
        self.results = []
        self.stop_flag.clear()
        self.error_occurred.clear()
        
        # Create queue and add batches
        batch_queue = queue.Queue()
        for batch in batches:
            batch_queue.put(batch)
        
        # Add poison pills for workers
        for _ in range(self.num_threads):
            batch_queue.put(None)
        
        total_batches = len(batches)
        self._report_progress(0, total_batches, "Starting parallel execution...")
        
        # Create and start worker threads
        threads = []
        for i in range(self.num_threads):
            thread = threading.Thread(
                target=self._worker,
                args=(batch_queue, total_batches),
                name=f"Worker-{i+1}"
            )
            thread.start()
            threads.append(thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check if execution completed successfully
        if self.error_occurred.is_set():
            self.logger.warning("Execution stopped due to error")
            self._report_progress(
                len(self.results),
                total_batches,
                "Execution failed - all transactions rolled back"
            )
        else:
            self._report_progress(
                total_batches,
                total_batches,
                f"All {total_batches} batches completed successfully!"
            )
        
        # Sort results by batch number
        self.results.sort(key=lambda x: x.batch_number)
        
        return self.results
    
    def get_summary(self) -> Dict:
        """Get execution summary statistics"""
        if not self.results:
            return {}
        
        successful = [r for r in self.results if r.success]
        failed = [r for r in self.results if not r.success]
        
        total_duration = sum(r.duration_seconds for r in self.results)
        total_rows = sum(r.rows_affected for r in successful)
        
        return {
            'total_batches': len(self.results),
            'successful': len(successful),
            'failed': len(failed),
            'total_rows_affected': total_rows,
            'total_duration_seconds': total_duration,
            'avg_duration_per_batch': total_duration / len(self.results) if self.results else 0
        }
