"""
SQL Script Parser - Splits batch scripts by GO statements
"""
import re
from typing import List


class SQLParser:
    """Parse SQL batch scripts and split by GO statements"""
    
    @staticmethod
    def parse_script(script_content: str) -> List[str]:
        """
        Parse SQL script and split by GO statements
        
        Args:
            script_content: The full SQL script content
            
        Returns:
            List of SQL batches (each batch is between GO statements)
        """
        # Remove comments but preserve line structure for better error reporting
        lines = script_content.split('\n')
        processed_lines = []
        in_multiline_comment = False
        
        for line in lines:
            # Handle multi-line comments /* */
            if '/*' in line:
                in_multiline_comment = True
                line = line[:line.index('/*')]
                
            if in_multiline_comment:
                if '*/' in line:
                    in_multiline_comment = False
                    line = line[line.index('*/') + 2:]
                else:
                    continue
                    
            # Handle single-line comments --
            if '--' in line:
                line = line[:line.index('--')]
                
            processed_lines.append(line)
        
        # Rejoin the script
        cleaned_script = '\n'.join(processed_lines)
        
        # Split by GO statement (case-insensitive, must be on its own line)
        # GO can have optional whitespace before/after
        batches = re.split(r'^\s*GO\s*$', cleaned_script, flags=re.MULTILINE | re.IGNORECASE)
        
        # Clean up batches - remove empty batches and trim whitespace
        cleaned_batches = []
        for i, batch in enumerate(batches):
            batch = batch.strip()
            if batch:  # Only keep non-empty batches
                cleaned_batches.append({
                    'batch_number': i + 1,
                    'sql': batch,
                    'line_count': batch.count('\n') + 1
                })
        
        return cleaned_batches
    
    @staticmethod
    def validate_script(script_content: str) -> tuple[bool, str]:
        """
        Validate SQL script for basic syntax issues
        
        Returns:
            Tuple of (is_valid, message)
        """
        if not script_content or not script_content.strip():
            return False, "Script is empty"
        
        # Check for balanced quotes
        single_quote_count = script_content.count("'") - script_content.count("\\'")
        if single_quote_count % 2 != 0:
            return False, "Unbalanced single quotes detected"
        
        # Check for basic SQL keywords to ensure it's likely SQL
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'EXEC', 'EXECUTE']
        has_sql = any(keyword in script_content.upper() for keyword in sql_keywords)
        
        if not has_sql:
            return False, "No SQL keywords detected - ensure this is a valid SQL script"
        
        return True, "Script validation passed"
    
    @staticmethod
    def get_script_stats(batches: List[dict]) -> dict:
        """Get statistics about parsed script"""
        total_lines = sum(b['line_count'] for b in batches)
        total_chars = sum(len(b['sql']) for b in batches)
        
        return {
            'total_batches': len(batches),
            'total_lines': total_lines,
            'total_characters': total_chars,
            'avg_lines_per_batch': total_lines / len(batches) if batches else 0
        }
