import os
import csv
import psycopg2

db_config = {
    'host': '127.0.0.1',
    'user': 'postgres',
    'password': '',
    'dbname': 'spider',
}

def create_table(table_name, columns):
    try:
        connection = psycopg2.connect(**db_config)
        with connection.cursor() as cursor:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
            cursor.execute(create_table_query)
        connection.commit()
    except Exception as e:
        raise Exception(f"Error creating table: {e}")
    finally:
        if connection:
            connection.close()

def determine_column_type_final(values):
    # Flags to track if any value is a float, a long integer, or a non-numeric value
    has_float = False
    has_long_int = False
    has_non_numeric = False

    # Iterate over values to determine type
    for value in values:
        if value:  # Check for non-empty string
            try:
                # Try converting to integer
                value_int = int(value)

                # Check if the integer is outside the 32-bit range
                if not (-2147483648 <= value_int <= 2147483647):
                    has_long_int = True
                    break
            except ValueError:
                # Not an integer, try converting to float
                try:
                    float(value)
                    has_float = True
                except ValueError:
                    # Not a float either, mark as non-numeric
                    has_non_numeric = True
                    break

    # Return the determined type
    if has_non_numeric or has_long_int:
        return 'VARCHAR(255)'
    elif has_float:
        return 'FLOAT'
    else:
        return 'INT'

def table_exists(cursor, table_name):
    cursor.execute(f"SELECT to_regclass('{table_name}')")
    return cursor.fetchone()[0] is not None

def process_csv_file(table_name, file_path):
    try:
        connection = psycopg2.connect(**db_config)
        with connection.cursor() as cursor:
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                columns = next(reader)
                column_values = [[] for _ in columns]
                for row in reader:
                    for i, value in enumerate(row):
                        column_values[i].append(value.strip())
                column_types = [determine_column_type_final(values) for values in column_values]
                if not table_exists(cursor, table_name):
                    create_table(table_name, [f'"{col}" {col_type}' for col, col_type in zip(columns, column_types)])
        connection.commit()
    except Exception as e:
        raise Exception(f"Error processing CSV file: {e}")
    finally:
        if connection:
            connection.close()

def explore_folder(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                folder_name = os.path.basename(root)
                table_name = f"{folder_name}_{os.path.splitext(file)[0]}"
                print("table_name:" + table_name)
                file_path = os.path.join(root, file)
                process_csv_file(table_name, file_path)

start_path = os.path.join(".", "spider")
if os.path.exists(start_path):
    explore_folder(start_path)
else:
    print(f"The specified folder '{start_path}' does not exist.")