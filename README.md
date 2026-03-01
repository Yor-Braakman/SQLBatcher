# SQL Batcher

Parallel SQL batch executor with multiple authentication methods for SQL Server and Azure SQL Database.

## Features

- Multiple authentication methods:
  - SQL Authentication (username/password)
  - Entra ID / MFA (interactive authentication)
  - Service Principal (client credentials)
- ODBC Driver selection (supports ODBC 17 and 18)
- Parse SQL scripts by GO statements
- Parallel execution with configurable thread count (1-64 threads)
- Stop-on-error with transaction rollback
- Progress tracking and execution logs
- Results table with batch-level details
- Export logs to file
- Windows executable available

## Prerequisites

- Python 3.8 or higher
- ODBC Driver 17 or 18 for SQL Server
- Windows OS (for executable)

### Install ODBC Driver

Download and install from Microsoft:
- [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- [ODBC Driver 17 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

## Setup for Development

### 1. Create Virtual Environment

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 2. Install Dependencies

```powershell
pip install -r requirements.txt
```

### 3. Run Application

```powershell
python main.py
```

## Usage

For Windows users, download & run the precompiled executable from the `dist` folder.

### 1. Database Connection

- Select ODBC driver (17 or 18)
- Choose authentication method
- Enter server and database details
- Configure encryption settings
- Test connection

### 2. Load SQL Script

- Browse for .sql file or paste directly
- Scripts should contain GO statements to separate batches
- Click "Parse Script" to split into batches

### 3. Execute

- Set number of parallel threads
- Click "Execute Batches"
- Monitor progress and view logs
- Review results in Results tab

### Authentication Methods

#### SQL Authentication
- Standard username/password
- Works with SQL Server authentication

#### Entra ID / MFA
- Interactive authentication with MFA support
- Optionally specify username
- Opens browser for authentication

#### Service Principal
- Automated authentication for applications
- Requires Tenant ID, Client ID, and Client Secret
- No interactive login required

## Building Windows Executable

### Build with PyInstaller

```powershell
.\venv\Scripts\Activate.ps1
pyinstaller --onefile --windowed --name="SQLBatcher" --icon=NONE main.py
```

The executable will be in the `dist` folder.

### Alternative: Use with Dependencies

```powershell
pyinstaller --onefile --windowed --name="SQLBatcher" ^
    --hidden-import=pyodbc ^
    --hidden-import=azure.identity ^
    --hidden-import=PyQt5 ^
    main.py
```

## Project Structure

```
SQLBatcher/
├── main.py                 # GUI application
├── db_connection.py        # Database connection module
├── sql_parser.py          # SQL script parser
├── parallel_executor.py   # Parallel execution engine
├── requirements.txt       # Python dependencies
├── README.md             # This file
└── .gitignore           # Git ignore rules
```

## Error Handling

- If any batch fails during execution, all threads stop immediately
- All transactions are rolled back
- Error details are displayed in the results table
- Full execution log available for debugging

## Limitations

- Windows only (due to PyQt5 and pyodbc dependencies)
- SQL Server / Azure SQL Database only
- GO statements must be on separate lines
- Maximum item size and connection limits apply per SQL Server configuration

## License

This project is provided as-is for use with SQL Server and Azure SQL Database.

## Contributing

Issues and pull requests welcome on GitHub.
