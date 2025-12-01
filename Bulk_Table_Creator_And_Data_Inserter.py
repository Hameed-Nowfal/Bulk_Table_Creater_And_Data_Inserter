import os
import pyodbc
import pandas as pd

from MS_SQL_SERVER_DB_Connection import connect_to_database
"Bulk insert"

def get_table_names(cursor):
    """Fetch all table names from the database."""
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    tables = cursor.fetchall()
    return [table[0] for table in tables]

def drop_table_if_exists(cursor, table_name):
    """Drop the specified table if it exists."""
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.commit()

def create_table(cursor, table_name, columns):
    """Create a table with the specified name and columns."""
    column_definitions = ', '.join([f'"{col}" NVARCHAR(MAX)' for col in columns])
    create_query = f'CREATE TABLE {table_name} ({column_definitions})'
    cursor.execute(create_query)
    cursor.commit()

def get_table_columns(cursor, table_name):
    """Fetch all column names from the specified table."""
    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' ORDER BY ORDINAL_POSITION")
    columns = cursor.fetchall()
    return [column[0] for column in columns]


def truncate_table(cursor, table_name):
    """Truncate the specified table."""
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    cursor.commit()


def bulk_insert_from_csv(cursor, table_name, csv_file_path):
    """Bulk insert data from CSV file into the specified table."""
    # Check if the first line contains "UTF-8" or "utf-8"
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        first_line = file.readline().strip()
        if 'utf-8' in first_line.lower():
            skip_rows = 1  # Skip the first line
            column_line = 1  # Column names are on the first line
        else:
            skip_rows = 0  # Skip the first two lines
            column_line = 1  # Column names are on the second line

    # Read the CSV file to get column names, skipping the appropriate number of lines
    df = pd.read_csv(csv_file_path, dtype=str, skiprows=skip_rows)

    # Replace NaN values with empty strings
    df = df.fillna('')

    # Get the column names from the SQL table
    table_columns = get_table_columns(cursor, table_name)

    # Ensure the DataFrame has the same number of columns as the SQL table
    if len(df.columns) != len(table_columns):
        raise ValueError(
            f"Column count mismatch: CSV has {len(df.columns)} columns, but SQL table has {len(table_columns)} columns.")

    # Enclose column names in double quotes
    columns = ', '.join([f'"{col}"' for col in table_columns])
    values = ', '.join(['?' for _ in table_columns])
    insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({values})'

    for index, row in df.iterrows():
        try:
            # Ensure all data is treated as string
            row_data = tuple(str(item) for item in row)
            cursor.execute(insert_query, row_data)
        except pyodbc.Error as e:
            print(f"Error inserting row {index}: {e}")
            print(f"Row data: {row_data}")
    cursor.commit()


def LoadData_From_csv_files(folder_path, cursor):
    """Process CSV files in the specified folder."""
    table_names = get_table_names(cursor)

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            table_name = file_name[:-4]  # Remove the ".csv" suffix
            if table_name in table_names:
                csv_file_path = os.path.join(folder_path, file_name)
                print(f"Processing file: {csv_file_path}")

                # Truncate the table
                truncate_table(cursor, table_name)

                # Bulk insert data from CSV
                bulk_insert_from_csv(cursor, table_name, csv_file_path)


def MakeTable_From_csv_files(folder_path, cursor):
    """Process CSV files in the specified folder."""
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv') and not file_name.startswith('Out_'):
            table_name = file_name[:-4]  # Remove the ".csv" suffix
            csv_file_path = os.path.join(folder_path, file_name)
            print(f"Processing file: {csv_file_path}")

            # Check if the first line contains "UTF-8" or "utf-8"
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                first_line = file.readline().strip()
                skip_rows = 1 if 'utf-8' in first_line.lower() else 0

            # Read the CSV file to get column names, skipping the first line if necessary
            df = pd.read_csv(csv_file_path, dtype=str, skiprows=skip_rows)

            # Handle duplicate column names
            columns = []
            column_count = {}
            for col in df.columns:
                if col in column_count:
                    column_count[col] += 1
                    columns.append(f"{col}{column_count[col]}")
                else:
                    column_count[col] = 1
                    columns.append(col)

            # Drop the table if it exists
            drop_table_if_exists(cursor, table_name)

            # Create the table with the columns
            create_table(cursor, table_name, columns)

# Input server and database name, Example -> Server Name: WORKSPA-B6JFO1N and DataBase Name: 1_DEV_ARB_DB OR 2_QA2_ARB_DB Or 3_QA1_ARB_DB OR 4_STG_ARB_DB
server_name = input("Enter server name: ")  
database_name = input("Enter database name: ")

# Establish connection
conn = connect_to_database(server_name, database_name)
cursor = conn.cursor()

# Folder path containing CSV files
folder_path = input("Enter the full folder path which contains the CSV files: ")

# Process CSV files
MakeTable_From_csv_files(folder_path, cursor)

# Process CSV files
LoadData_From_csv_files(folder_path, cursor)

# Close the database connection
cursor.close()
conn.close()
