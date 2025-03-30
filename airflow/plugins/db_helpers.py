from db import db_config as db

conn = db.get_connection()

def check_table_existency(tablename):
    try:
        query = f'''select exists(
                    select 1
                    from pg_tables
                    where tablename = {tablename}
                ) as table_existence
                '''
        result = db.execute_query(query, fetch_one=True)
        return result[0]
    except Exception as e:
        return print(f'Error checking table. Error: {e}')
    
def create_table(tablename):
    try:
        query = '''CREATE TABLE test_table(
                    id serial primary key,
                    name varchar(255)
                    )
                '''
        table_exists = check_table_existency(tablename)
        
        if table_exists:
            return print(f'Table {tablename} already exists')

        result = db.execute_query(query)
        return print(f'Table {tablename} created')
    
    except Exception as e:
        return print(f'Error creating table. Error: {e}')

