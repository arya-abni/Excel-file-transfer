from flask import Flask, render_template, request, redirect
from sqlalchemy import create_engine, exc
from sqlalchemy.engine import URL
import pypyodbc
import pandas as pd

app = Flask(__name__)

SERVER_NAME = '172.20.20.251'
USERNAME = 'sa'  
PASSWORD = 'Ccrd$1234' 
# SERVER_NAME=r'ABNISH\SQLEXPRESS'

# USERNAME='radhe'
# PASSWORD='abnish@arya8955'


connection_string = f"""
    DRIVER={{SQL Server}};
    SERVER={SERVER_NAME};
    UID={USERNAME};
    PWD={PASSWORD};
   
"""

conn = pypyodbc.connect(connection_string)
cursor = conn.cursor()
databases_query = "SELECT name FROM master.sys.databases WHERE database_id > 4"
cursor.execute(databases_query)

# databases = [db[0] for db in cursor.fetchall()]

databases= ['Abnish', 'Ram']

database_tables_dict = {}

# Iterate through databases and retrieve tables
for db_name in databases:
    # Use the database
    use_db_query = f"USE [{db_name}]"
    cursor.execute(use_db_query)

    # Retrieve table names
    tables_query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
    cursor.execute(tables_query)
    tables = [table[0] for table in cursor.fetchall()]

    database_tables_dict[db_name] = tables

 


cursor.close()
conn.close()

text_file_path = 'tables.txt'

values = list(database_tables_dict.values())

# Extract words from the nested list
result_string = [word for sublist in values if sublist for word in sublist if isinstance(word, str)]

with open(text_file_path, 'w') as textfile:
    textfile.write('\n'.join(result_string))


connection_url = URL.create('mssql+pyodbc', query={'odbc_connect': connection_string})
engine = create_engine(connection_url, module=pypyodbc)


my_table = ""  # Initialize the my_table variable
#

@app.route('/', methods=['GET', 'POST'])
def index():
    global my_table

    if request.method == 'POST':
        my_table = request.form['selected_table']

    with open('tables.txt', 'r') as file:
        options = [line.strip() for line in file]

    # Add a default option
    options.insert(0, "<--select-->")

    return render_template('index.html', options=options, my_table=my_table)


@app.route('/upload', methods=['POST'])
def upload():
    global my_table

    if request.method == 'POST':
        my_table = request.form['selected_table']
        DATABASE_NAME=""
        for key, val in database_tables_dict.items():
            if val == my_table:
                DATABASE_NAME=key
                print(DATABASE_NAME)
                break

        print(f'my_table: {my_table}, database_tables_dict: {database_tables_dict}')
        for key, val in database_tables_dict.items():
            print(f'Comparing my_table: {my_table} and val: {val}')
            if my_table in val:
                DATABASE_NAME = key
                print(f'Matched: my_table - {my_table}, DATABASE_NAME - {DATABASE_NAME}')
                break
        SERVER_NAME = '172.20.20.251'

        connection_string = f"""
            DRIVER={{SQL Server}};
            SERVER={SERVER_NAME};
            DATABASE={DATABASE_NAME};
            UID={USERNAME};
            PWD={PASSWORD};
            
        """
        connection_url = URL.create('mssql+pyodbc', query={'odbc_connect': connection_string})
        engine = create_engine(connection_url, module=pypyodbc)


        if 'file' not in request.files:
            return redirect(request.url)

        file = request.files['file']

        if file.filename == '':
            return redirect(request.url)
        if file:
            try:
                df = pd.read_excel(file, sheet_name=None, engine='openpyxl')

                for sheet_name, df_data in df.items():
                    print(f'Loading worksheet {sheet_name}...')
                    
                    # {'fail', 'replace', 'append'}
                    df_data.to_sql(my_table, engine, if_exists='append', index=False)
                    print("dataloaded sucessfully")

                return 'Data loaded successfully✅'
            except exc.SQLAlchemyError as e:
                print(f'Error during to_sql operation: {e}')
                return '❌ You are entring a file that have columns different from the columns that are on your database. Please recheck your column name and try again!'


if __name__ == "__main__":
    app.run(debug=True)
