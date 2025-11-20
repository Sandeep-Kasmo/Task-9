import pandas as pd #type:ignore
import mysql.connector #type:ignore
from transform import *
from extract import *

def load_data(dataframe,table_name):
    global conn
    cursor=None
    if conn is None or conn.is_connected()==False:
        try:
            conn=establish_connection()
        except Error as e:
            print('Error:{e}')
            return False
    
    try:
        cursor=conn.cursor()
        cursor.execute(f'drop table if exists {table_name}')
        columns_def=[]      
        for col in dataframe.columns:
            dtype = dataframe[col].dtype
            if "int" in str(dtype):
                col_type = "INT"
            elif "float" in str(dtype):
                col_type = "FLOAT"
            elif "datetime" in str(dtype):
                col_type = "DATETIME"
            else:
                col_type = "VARCHAR(255)"

            columns_def.append(f"`{col}` {col_type}")
        create_sql=f"""
create table {table_name}(
{','.join(columns_def)})
        """
        cursor.execute(create_sql)

        if not dataframe.empty:
            placeholders=", ".join(['%s']*len(dataframe.columns))
            insert_sql=f"""insert into {table_name} 
            ({', '.join([f'`{c}`' for c in dataframe.columns])})
            values ({placeholders})"""

            data=[tuple(row) for row in dataframe.to_numpy()]
            cursor.executemany(insert_sql,data)
            conn.commit()

        print(f"Table {table_name} created succesfully")
        return True
    except Error as e:
        print(f"Error creating table {table_name}:{e}")
        return False
    finally:
        if cursor:
            cursor.close()



    

        
