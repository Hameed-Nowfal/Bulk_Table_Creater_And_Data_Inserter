import pyodbc

def connect_to_database(server, database):
    try:
        # Establish connection
        conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                              f'SERVER={server};'
                              f'DATABASE={database};'
                              'Trusted_Connection=yes;')
 
        print("Connected to the database successfully.")
        return conn
    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")
        return None
    "MS SQL"
