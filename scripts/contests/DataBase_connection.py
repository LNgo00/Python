
import mysql.connector as db
from mysql.connector import Error

def open_connection():

    try:

        import mysql.connector as db

        return db.connect(user='root', passwd = '', host='localhost', database='cryptobirds', port=3307)

    except db.Error as error:
        
        print("failed to connect to database: {}".format(error))
    
def close_connection(connection):
    
    if connection.is_connected():
        connection.close()


if __name__ == '__main__':

    print('step1')
    print()
    connection = open_connection()
    print('connected to db')
    print()
    print('step 2')
    print()
    close_connection(connection)
    print('connection closed')
