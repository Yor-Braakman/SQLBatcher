"""
SQL Batcher - Main GUI Application
Parallel SQL Batch Executor with Multiple Authentication Methods
"""
import sys
import os
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QComboBox, QPushButton, QTextEdit, QFileDialog,
    QSpinBox, QGroupBox, QProgressBar, QTabWidget, QTableWidget,
    QTableWidgetItem, QMessageBox, QCheckBox
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont
from datetime import datetime
import logging

from db_connection import DatabaseConnection
from sql_parser import SQLParser
from parallel_executor import ParallelExecutor, ExecutionResult


class ExecutorThread(QThread):
    """Thread for executing SQL batches without blocking UI"""
    progress = pyqtSignal(int, int, str)
    finished = pyqtSignal(list, dict)
    
    def __init__(self, connection_string: str, batches: list, num_threads: int):
        super().__init__()
        self.connection_string = connection_string
        self.batches = batches
        self.num_threads = num_threads
        
    def run(self):
        """Execute batches in background thread"""
        executor = ParallelExecutor(self.connection_string, self.num_threads)
        executor.set_progress_callback(self._progress_callback)
        
        results = executor.execute_batches(self.batches)
        summary = executor.get_summary()
        
        self.finished.emit(results, summary)
    
    def _progress_callback(self, completed: int, total: int, message: str):
        """Forward progress to main thread"""
        self.progress.emit(completed, total, message)


class MainWindow(QMainWindow):
    """Main application window"""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("SQL Batcher - Parallel SQL Executor")
        self.setMinimumSize(1200, 800)
        
        self.db_connection = DatabaseConnection()
        self.parsed_batches = []
        self.executor_thread = None
        self.log_filename = ""
        
        self._init_ui()
        self._load_drivers()
        
    def _init_ui(self):
        """Initialize the user interface"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)
        
        # Create tabs
        tabs = QTabWidget()
        main_layout.addWidget(tabs)
        
        # Tab 1: Connection
        connection_tab = self._create_connection_tab()
        tabs.addTab(connection_tab, "Database Connection")
        
        # Tab 2: Script
        script_tab = self._create_script_tab()
        tabs.addTab(script_tab, "SQL Script")
        
        # Tab 3: Execution
        execution_tab = self._create_execution_tab()
        tabs.addTab(execution_tab, "Execution")
        
        # Tab 4: Results
        results_tab = self._create_results_tab()
        tabs.addTab(results_tab, "Results")
        
        # Status bar
        self.statusBar().showMessage("Ready")
        
    def _create_connection_tab(self):
        """Create database connection tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # Driver selection
        driver_group = QGroupBox("ODBC Driver")
        driver_layout = QHBoxLayout()
        driver_group.setLayout(driver_layout)
        
        driver_layout.addWidget(QLabel("Driver:"))
        self.driver_combo = QComboBox()
        driver_layout.addWidget(self.driver_combo, 1)
        layout.addWidget(driver_group)
        
        # Authentication method
        auth_group = QGroupBox("Authentication Method")
        auth_layout = QVBoxLayout()
        auth_group.setLayout(auth_layout)
        
        self.auth_combo = QComboBox()
        self.auth_combo.addItems([
            DatabaseConnection.AUTH_SQL,
            DatabaseConnection.AUTH_AZURE_AD,
            DatabaseConnection.AUTH_SERVICE_PRINCIPAL
        ])
        self.auth_combo.currentTextChanged.connect(self._on_auth_method_changed)
        auth_layout.addWidget(self.auth_combo)
        layout.addWidget(auth_group)
        
        # Server and Database
        server_group = QGroupBox("Server Configuration")
        server_layout = QVBoxLayout()
        server_group.setLayout(server_layout)
        
        server_layout.addWidget(QLabel("Server:"))
        self.server_input = QLineEdit()
        self.server_input.setPlaceholderText("e.g., myserver.database.windows.net")
        server_layout.addWidget(self.server_input)
        
        server_layout.addWidget(QLabel("Database:"))
        self.database_input = QLineEdit()
        self.database_input.setPlaceholderText("e.g., mydb")
        server_layout.addWidget(self.database_input)
        
        # Encryption options
        encryption_layout = QHBoxLayout()
        self.encrypt_check = QCheckBox("Encrypt Connection")
        self.encrypt_check.setChecked(True)
        encryption_layout.addWidget(self.encrypt_check)
        
        self.trust_cert_check = QCheckBox("Trust Server Certificate")
        self.trust_cert_check.setChecked(False)
        encryption_layout.addWidget(self.trust_cert_check)
        server_layout.addLayout(encryption_layout)
        
        layout.addWidget(server_group)
        
        # SQL Authentication fields
        self.sql_auth_group = QGroupBox("SQL Authentication")
        sql_auth_layout = QVBoxLayout()
        self.sql_auth_group.setLayout(sql_auth_layout)
        
        sql_auth_layout.addWidget(QLabel("Username:"))
        self.username_input = QLineEdit()
        sql_auth_layout.addWidget(self.username_input)
        
        sql_auth_layout.addWidget(QLabel("Password:"))
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.Password)
        sql_auth_layout.addWidget(self.password_input)
        
        layout.addWidget(self.sql_auth_group)
        
        # Entra ID fields
        self.azure_ad_group = QGroupBox("Entra ID Authentication")
        azure_ad_layout = QVBoxLayout()
        self.azure_ad_group.setLayout(azure_ad_layout)
        
        azure_ad_layout.addWidget(QLabel("Username (optional):"))
        self.azure_username_input = QLineEdit()
        self.azure_username_input.setPlaceholderText("user@domain.com")
        azure_ad_layout.addWidget(self.azure_username_input)
        
        layout.addWidget(self.azure_ad_group)
        self.azure_ad_group.setVisible(False)
        
        # Service Principal fields
        self.sp_group = QGroupBox("Service Principal")
        sp_layout = QVBoxLayout()
        self.sp_group.setLayout(sp_layout)
        
        sp_layout.addWidget(QLabel("Tenant ID:"))
        self.tenant_input = QLineEdit()
        sp_layout.addWidget(self.tenant_input)
        
        sp_layout.addWidget(QLabel("Client ID:"))
        self.client_id_input = QLineEdit()
        sp_layout.addWidget(self.client_id_input)
        
        sp_layout.addWidget(QLabel("Client Secret:"))
        self.client_secret_input = QLineEdit()
        self.client_secret_input.setEchoMode(QLineEdit.Password)
        sp_layout.addWidget(self.client_secret_input)
        
        layout.addWidget(self.sp_group)
        self.sp_group.setVisible(False)
        
        # Test connection button
        test_btn = QPushButton("Test Connection")
        test_btn.clicked.connect(self._test_connection)
        layout.addWidget(test_btn)
        
        # Connection status
        self.connection_status = QTextEdit()
        self.connection_status.setReadOnly(True)
        self.connection_status.setMaximumHeight(100)
        layout.addWidget(self.connection_status)
        
        layout.addStretch()
        return widget
    
    def _create_script_tab(self):
        """Create SQL script tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # File selection
        file_layout = QHBoxLayout()
        file_layout.addWidget(QLabel("Script File:"))
        self.file_input = QLineEdit()
        self.file_input.setReadOnly(True)
        file_layout.addWidget(self.file_input, 1)
        
        browse_btn = QPushButton("Browse...")
        browse_btn.clicked.connect(self._browse_script_file)
        file_layout.addWidget(browse_btn)
        
        load_btn = QPushButton("Load Script")
        load_btn.clicked.connect(self._load_script)
        file_layout.addWidget(load_btn)
        
        layout.addLayout(file_layout)
        
        # Script content
        layout.addWidget(QLabel("Script Content:"))
        self.script_text = QTextEdit()
        self.script_text.setPlaceholderText("Load a SQL script file or paste your SQL here...")
        font = QFont("Consolas", 10)
        self.script_text.setFont(font)
        layout.addWidget(self.script_text)
        
        # Parse button
        parse_btn = QPushButton("Parse Script (Split by GO)")
        parse_btn.clicked.connect(self._parse_script)
        layout.addWidget(parse_btn)
        
        # Parse results
        self.parse_results = QTextEdit()
        self.parse_results.setReadOnly(True)
        self.parse_results.setMaximumHeight(100)
        layout.addWidget(self.parse_results)
        
        return widget
    
    def _create_execution_tab(self):
        """Create execution configuration tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # Execution settings
        settings_group = QGroupBox("Execution Settings")
        settings_layout = QVBoxLayout()
        settings_group.setLayout(settings_layout)
        
        threads_layout = QHBoxLayout()
        threads_layout.addWidget(QLabel("Number of Parallel Threads:"))
        self.threads_spin = QSpinBox()
        self.threads_spin.setMinimum(1)
        self.threads_spin.setMaximum(64)
        self.threads_spin.setValue(4)
        threads_layout.addWidget(self.threads_spin)
        threads_layout.addStretch()
        settings_layout.addLayout(threads_layout)
        
        settings_layout.addWidget(QLabel("Note: Execution will stop immediately if any batch fails. All transactions will be rolled back."))
        
        layout.addWidget(settings_group)
        
        # Execute button
        self.execute_btn = QPushButton("Execute Batches")
        self.execute_btn.clicked.connect(self._execute_batches)
        self.execute_btn.setEnabled(False)
        layout.addWidget(self.execute_btn)
        
        # Progress
        layout.addWidget(QLabel("Progress:"))
        self.progress_bar = QProgressBar()
        layout.addWidget(self.progress_bar)
        
        # Execution log
        layout.addWidget(QLabel("Execution Log:"))
        self.execution_log = QTextEdit()
        self.execution_log.setReadOnly(True)
        font = QFont("Consolas", 9)
        self.execution_log.setFont(font)
        layout.addWidget(self.execution_log)
        
        # Save log button
        save_log_btn = QPushButton("Save Log to File")
        save_log_btn.clicked.connect(self._save_log)
        layout.addWidget(save_log_btn)
        
        return widget
    
    def _create_results_tab(self):
        """Create results tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # Summary
        self.summary_text = QTextEdit()
        self.summary_text.setReadOnly(True)
        self.summary_text.setMaximumHeight(150)
        layout.addWidget(QLabel("Execution Summary:"))
        layout.addWidget(self.summary_text)
        
        # Results table
        layout.addWidget(QLabel("Batch Results:"))
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(6)
        self.results_table.setHorizontalHeaderLabels([
            "Batch #", "Status", "Rows Affected", "Duration (s)", "Timestamp", "Error"
        ])
        self.results_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.results_table)
        
        return widget
    
    def _load_drivers(self):
        """Load available ODBC drivers"""
        drivers = DatabaseConnection.get_available_drivers()
        self.driver_combo.clear()
        
        # Prefer ODBC 18 and 17
        preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
        for driver in preferred:
            if driver in drivers:
                self.driver_combo.addItem(driver)
        
        # Add others
        for driver in drivers:
            if driver not in preferred:
                self.driver_combo.addItem(driver)
        
        if self.driver_combo.count() == 0:
            self.driver_combo.addItem("No ODBC drivers found")
            self.statusBar().showMessage("WARNING: No SQL Server ODBC drivers found!")
    
    def _on_auth_method_changed(self, method: str):
        """Handle authentication method change"""
        self.sql_auth_group.setVisible(method == DatabaseConnection.AUTH_SQL)
        self.azure_ad_group.setVisible(method == DatabaseConnection.AUTH_AZURE_AD)
        self.sp_group.setVisible(method == DatabaseConnection.AUTH_SERVICE_PRINCIPAL)
    
    def _test_connection(self):
        """Test database connection"""
        try:
            self.connection_status.clear()
            self.connection_status.append("Testing connection...")
            QApplication.processEvents()
            
            auth_method = self.auth_combo.currentText()
            driver = self.driver_combo.currentText()
            
            # Get connection parameters
            server = self.server_input.text().strip()
            database = self.database_input.text().strip()
            
            if not server or not database:
                self.connection_status.append("\nError: Server and Database are required")
                return
            
            # Close existing connection
            self.db_connection.close()
            
            # Connect based on auth method
            kwargs = {
                'server': server,
                'database': database,
                'auth_method': auth_method,
                'driver': driver,
                'encrypt': self.encrypt_check.isChecked(),
                'trust_cert': self.trust_cert_check.isChecked()
            }
            
            if auth_method == DatabaseConnection.AUTH_SQL:
                kwargs['username'] = self.username_input.text().strip()
                kwargs['password'] = self.password_input.text()
                
            elif auth_method == DatabaseConnection.AUTH_AZURE_AD:
                kwargs['username'] = self.azure_username_input.text().strip()
                
            elif auth_method == DatabaseConnection.AUTH_SERVICE_PRINCIPAL:
                kwargs['tenant_id'] = self.tenant_input.text().strip()
                kwargs['client_id'] = self.client_id_input.text().strip()
                kwargs['client_secret'] = self.client_secret_input.text()
            
            self.db_connection.connect(**kwargs)
            success, message = self.db_connection.test_connection()
            
            if success:
                self.connection_status.append(f"\nSUCCESS: {message}")
                self.statusBar().showMessage("Connection successful!")
                self._update_execute_button_state()
            else:
                self.connection_status.append(f"\nERROR: {message}")
                self.statusBar().showMessage("Connection failed")
                
        except Exception as e:
            self.connection_status.append(f"\nERROR: Connection error: {str(e)}")
            self.statusBar().showMessage("Connection failed")
    
    def _browse_script_file(self):
        """Browse for SQL script file"""
        filename, _ = QFileDialog.getOpenFileName(
            self,
            "Select SQL Script",
            "",
            "SQL Files (*.sql);;All Files (*.*)"
        )
        if filename:
            self.file_input.setText(filename)
    
    def _load_script(self):
        """Load SQL script from file"""
        filename = self.file_input.text()
        if not filename:
            QMessageBox.warning(self, "No File", "Please select a script file first")
            return
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
            self.script_text.setPlainText(content)
            self.statusBar().showMessage(f"Loaded script from {filename}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load script:\n{str(e)}")
    
    def _parse_script(self):
        """Parse SQL script and split by GO statements"""
        script_content = self.script_text.toPlainText()
        
        if not script_content.strip():
            QMessageBox.warning(self, "No Script", "Please load or enter a SQL script first")
            return
        
        try:
            # Validate script
            valid, message = SQLParser.validate_script(script_content)
            if not valid:
                QMessageBox.warning(self, "Validation Failed", message)
                return
            
            # Parse script
            self.parsed_batches = SQLParser.parse_script(script_content)
            
            if not self.parsed_batches:
                self.parse_results.setText("No batches found. Make sure your script contains GO statements.")
                return
            
            # Get statistics
            stats = SQLParser.get_script_stats(self.parsed_batches)
            
            result_text = f"""
Script parsed successfully!

Total Batches: {stats['total_batches']}
Total Lines: {stats['total_lines']}
Total Characters: {stats['total_characters']:,}
Average Lines per Batch: {stats['avg_lines_per_batch']:.1f}
            """
            
            self.parse_results.setText(result_text.strip())
            self.statusBar().showMessage(f"Parsed {stats['total_batches']} batches")
            self._update_execute_button_state()
            
        except Exception as e:
            QMessageBox.critical(self, "Parse Error", f"Failed to parse script:\n{str(e)}")
    
    def _update_execute_button_state(self):
        """Enable/disable execute button based on state"""
        has_connection = self.db_connection.get_connection() is not None
        has_batches = len(self.parsed_batches) > 0
        self.execute_btn.setEnabled(has_connection and has_batches)
    
    def _execute_batches(self):
        """Execute SQL batches in parallel"""
        if not self.db_connection.get_connection():
            QMessageBox.warning(self, "No Connection", "Please connect to database first")
            return
        
        if not self.parsed_batches:
            QMessageBox.warning(self, "No Batches", "Please parse a SQL script first")
            return
        
        # Confirm execution
        reply = QMessageBox.question(
            self,
            "Confirm Execution",
            f"Execute {len(self.parsed_batches)} batches with {self.threads_spin.value()} parallel threads?\n\n"
            "Note: If any batch fails, ALL transactions will be rolled back.",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
        
        # Disable execute button
        self.execute_btn.setEnabled(False)
        self.execution_log.clear()
        self.progress_bar.setValue(0)
        
        # Start execution in background thread
        num_threads = self.threads_spin.value()
        connection_string = self.db_connection.connection_string
        
        self.executor_thread = ExecutorThread(connection_string, self.parsed_batches, num_threads)
        self.executor_thread.progress.connect(self._on_execution_progress)
        self.executor_thread.finished.connect(self._on_execution_finished)
        self.executor_thread.start()
        
        self.statusBar().showMessage("Executing batches...")
        self._log_message(f"Started execution at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._log_message(f"Batches: {len(self.parsed_batches)}, Threads: {num_threads}")
    
    def _on_execution_progress(self, completed: int, total: int, message: str):
        """Handle progress update from executor"""
        progress = int((completed / total) * 100) if total > 0 else 0
        self.progress_bar.setValue(progress)
        self._log_message(message)
    
    def _on_execution_finished(self, results: list, summary: dict):
        """Handle execution completion"""
        self.execute_btn.setEnabled(True)
        
        # Log summary
        self._log_message(f"Execution finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._log_message(f"Total: {summary['total_batches']}, "
                         f"Successful: {summary['successful']}, "
                         f"Failed: {summary['failed']}")
        
        if summary['failed'] > 0:
            self.statusBar().showMessage("Execution failed - transactions rolled back")
            QMessageBox.warning(
                self,
                "Execution Failed",
                f"Execution stopped due to error.\n"
                f"Batches executed: {summary['total_batches']}\n"
                f"Failed batches: {summary['failed']}\n\n"
                f"All transactions have been rolled back."
            )
        else:
            self.statusBar().showMessage("Execution completed successfully!")
            QMessageBox.information(
                self,
                "Success",
                f"All {summary['successful']} batches executed successfully!\n"
                f"Total rows affected: {summary['total_rows_affected']:,}\n"
                f"Total duration: {summary['total_duration_seconds']:.2f} seconds"
            )
        
        # Display results
        self._display_results(results, summary)
    
    def _display_results(self, results: list, summary: dict):
        """Display execution results in results tab"""
        # Summary
        summary_text = f"""
Execution Summary:
------------------
Total Batches: {summary['total_batches']}
Successful: {summary['successful']}
Failed: {summary['failed']}
Total Rows Affected: {summary['total_rows_affected']:,}
Total Duration: {summary['total_duration_seconds']:.2f} seconds
Average Duration per Batch: {summary['avg_duration_per_batch']:.2f} seconds
        """
        self.summary_text.setText(summary_text.strip())
        
        # Results table
        self.results_table.setRowCount(len(results))
        for i, result in enumerate(results):
            self.results_table.setItem(i, 0, QTableWidgetItem(str(result.batch_number)))
            self.results_table.setItem(i, 1, QTableWidgetItem("Success" if result.success else "Failed"))
            self.results_table.setItem(i, 2, QTableWidgetItem(str(result.rows_affected)))
            self.results_table.setItem(i, 3, QTableWidgetItem(f"{result.duration_seconds:.2f}"))
            self.results_table.setItem(i, 4, QTableWidgetItem(result.timestamp))
            self.results_table.setItem(i, 5, QTableWidgetItem(result.error_message))
        
        self.results_table.resizeColumnsToContents()
    
    def _log_message(self, message: str):
        """Add message to execution log"""
        self.execution_log.append(message)
        self.execution_log.verticalScrollBar().setValue(
            self.execution_log.verticalScrollBar().maximum()
        )
    
    def _save_log(self):
        """Save execution log to file"""
        log_content = self.execution_log.toPlainText()
        if not log_content:
            QMessageBox.information(self, "No Log", "No execution log to save")
            return
        
        filename, _ = QFileDialog.getSaveFileName(
            self,
            "Save Execution Log",
            f"execution_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            "Text Files (*.txt);;All Files (*.*)"
        )
        
        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(log_content)
                QMessageBox.information(self, "Saved", f"Log saved to:\n{filename}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to save log:\n{str(e)}")


def main():
    """Main entry point"""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")  # Modern look
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
