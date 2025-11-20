import pandas as pd #type:ignore
import mysql.connector #type:ignore
from mysql.connector import Error #type:ignore
conn=None

def establish_connection():
    try:
        global conn
        conn=mysql.connector.connect(
            host='localhost',
            user='root',
            password='**********',
            database='PythonLearningDB'
        )
        if conn.is_connected():
            print('Connected to MySQL Server\n')
            return conn
    except Error as e:
        print(f'Error connecting to SQL:{e}')
        return None
    
def get_data(query):
    try:
        if conn is None or conn.is_connected()==False:
            try:
                establish_connection()
            except Error as e:
                print(f'Error:{e}')
                return None
        dataframe=pd.read_sql(query,conn)
        return dataframe
    except Error as e:
        print(f"Error extracting {dataframe}:{e}")
        return None

def close_connection():
    global conn
    try:
        if conn and conn.is_connected():
            conn.close()
            print('Closed connection successfully!')
    except Error as e:
        print(f"Error closing connection:{e}")
        return None


