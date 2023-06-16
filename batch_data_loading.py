#Load Data into PostgreSQL Database using Batch ID with Date Correction
import psycopg2
import os
from datetime import datetime

def load_data():
    """
    Documentation: Load data from files into a PostgreSQL database using batch ID.

    Note: Assumes the batch ID is 1 in the file_batch table.
    """

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        database="processed_shipping",
        user="postgres",
        password="apple123",
        host="localhost",
        port="5432"
    )

    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    # Fetch the file paths and layout IDs for batch ID = 1 from the file_batch and files_info tables
    file_info_query = """
        SELECT fi.file_path, fi.layout_id
        FROM file_batch fb
        JOIN files_info fi ON fb.file_id = fi.id
        WHERE fb.batch_id = 1;
    """
    cursor.execute(file_info_query)
    file_info_rows = cursor.fetchall()

    for file_info in file_info_rows:
        file_path, layout_id = file_info

        # Extract the file name from the file path
        file_name = os.path.basename(file_path)

        # Prepare the query to fetch the header names and data types for the specified layout ID
        header_query = f"SELECT column_name, datatype FROM layout_table1 WHERE layout_id = {layout_id};"
        cursor.execute(header_query)
        columns = cursor.fetchall()

        # Create a temporary table in the database based on the specified layout ID and header names
        temp_table_name = f"temp_layout_{layout_id}"
        create_table_query = f"CREATE TEMP TABLE IF NOT EXISTS {temp_table_name} ("

        # Generate column definitions with data types
        column_definitions = []
        for column in columns:
            column_name, data_type = column
            column_definitions.append(f"{column_name} {data_type}")

        create_table_query += ", ".join(column_definitions)
        create_table_query += ");"

        cursor.execute(create_table_query)

        # Prepare the query to copy data from the file into the temporary table
        copy_query = f"""
            COPY {temp_table_name} FROM STDIN WITH (
                FORMAT csv,
                DELIMITER '|',
                HEADER TRUE,
                NULL ''
            );
        """

        # Open the file and copy its content into the temporary table
        with open(file_path, 'r') as file:
            cursor.copy_expert(copy_query, file)

        # Check if the 'date_of_birth' column exists in the temporary table
        date_of_birth_exists = 'date_of_birth' in [column[0] for column in columns]
        if date_of_birth_exists:
           current_year = datetime.now().year
           update_query = f"""
              UPDATE {temp_table_name}
              SET date_of_birth = CASE
                  WHEN EXTRACT(YEAR FROM date_of_birth) > {current_year}
                  THEN date_of_birth - INTERVAL '100 years'
                  ELSE date_of_birth
              END;
           """

           cursor.execute(update_query)
    

        # Create the final table based on the layout ID and copy the corrected data from the temporary table
        final_table_name = f"layout_{layout_id}"
        create_final_table_query = f"CREATE TABLE IF NOT EXISTS {final_table_name} AS TABLE {temp_table_name};"
        cursor.execute(create_final_table_query)

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and the connection
    cursor.close()
    conn.close()