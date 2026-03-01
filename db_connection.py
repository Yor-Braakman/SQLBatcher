"""
Database connection module with multiple authentication methods for SQL Server
"""
import pyodbc
from azure.identity import DefaultAzureCredential, ClientSecretCredential
import struct
from typing import Optional, Dict


class DatabaseConnection:
    """Handles SQL Server connections with multiple authentication methods"""
    
    AUTH_SQL = "SQL Authentication"
    AUTH_AZURE_AD = "Entra ID / MFA"
    AUTH_SERVICE_PRINCIPAL = "Service Principal"
    
    def __init__(self):
        self.connection: Optional[pyodbc.Connection] = None
        self.connection_string: str = ""
        
    @staticmethod
    def get_available_drivers():
        """Get list of available ODBC drivers"""
        drivers = [d for d in pyodbc.drivers() if 'SQL Server' in d]
        return drivers
    
    def build_connection_string(
        self,
        server: str,
        database: str,
        auth_method: str,
        driver: str = "ODBC Driver 18 for SQL Server",
        username: str = "",
        password: str = "",
        tenant_id: str = "",
        client_id: str = "",
        client_secret: str = "",
        encrypt: bool = True,
        trust_cert: bool = False
    ) -> str:
        """Build connection string based on authentication method"""
        
        # Base connection string
        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
        
        # Encryption settings
        if encrypt:
            conn_str += "Encrypt=yes;"
        if trust_cert:
            conn_str += "TrustServerCertificate=yes;"
            
        # Authentication specific settings
        if auth_method == self.AUTH_SQL:
            conn_str += f"UID={username};PWD={password};"
            
        elif auth_method == self.AUTH_AZURE_AD:
            # Entra ID Interactive (MFA)
            conn_str += "Authentication=ActiveDirectoryInteractive;"
            if username:
                conn_str += f"UID={username};"
                
        elif auth_method == self.AUTH_SERVICE_PRINCIPAL:
            # Service Principal - will use access token
            conn_str += "Authentication=ActiveDirectoryServicePrincipal;"
            conn_str += f"UID={client_id};PWD={client_secret};"
            
        return conn_str
    
    def get_azure_token(self, tenant_id: str, client_id: str, client_secret: str) -> bytes:
        """Get Entra ID access token for Service Principal authentication"""
        if client_id and client_secret:
            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        else:
            credential = DefaultAzureCredential()
            
        # Get token for SQL Database
        token = credential.get_token("https://database.windows.net/.default")
        
        # Convert token to the format expected by pyodbc
        token_bytes = token.token.encode("utf-16-le")
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
        
        return token_struct
    
    def connect(
        self,
        server: str,
        database: str,
        auth_method: str,
        driver: str = "ODBC Driver 18 for SQL Server",
        username: str = "",
        password: str = "",
        tenant_id: str = "",
        client_id: str = "",
        client_secret: str = "",
        encrypt: bool = True,
        trust_cert: bool = False
    ) -> None:
        """Establish database connection"""
        
        self.connection_string = self.build_connection_string(
            server, database, auth_method, driver,
            username, password, tenant_id, client_id, client_secret,
            encrypt, trust_cert
        )
        
        # For Entra ID with token (alternative method)
        if auth_method == self.AUTH_SERVICE_PRINCIPAL and tenant_id:
            try:
                # Try using access token directly
                token_struct = self.get_azure_token(tenant_id, client_id, client_secret)
                # Build connection string without authentication
                conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                if encrypt:
                    conn_str += "Encrypt=yes;"
                if trust_cert:
                    conn_str += "TrustServerCertificate=yes;"
                    
                self.connection = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
                return
            except Exception:
                # Fall back to connection string authentication
                pass
        
        # Standard connection
        self.connection = pyodbc.connect(self.connection_string)
        
    def test_connection(self) -> tuple[bool, str]:
        """Test the database connection"""
        try:
            if not self.connection:
                return False, "No connection established"
                
            cursor = self.connection.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            cursor.close()
            
            return True, f"Connected successfully!\n{version[:100]}..."
            
        except Exception as e:
            return False, f"Connection failed: {str(e)}"
    
    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def get_connection(self) -> Optional[pyodbc.Connection]:
        """Get the active connection"""
        return self.connection
