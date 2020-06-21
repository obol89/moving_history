import mysql.connector
from mysql.connector import errorcode
import argparse

select_some_records = """
select * from first"""


def select_records(database1, user_database1, host_database1, password_database1, database2, user_database2, host_database2, password_database2):
    print(database1, user_database1, host_database1, password_database1, database2, user_database2, host_database2, password_database2)
    try:
        cnx = mysql.connector.connect(database=database1, user=user_database1, host=host_database1, password=password_database1)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database {} does not exist".format(database1))
        else:
            print(err)
    else:
        mycursor = cnx.cursor()
        mycursor.execute(select_some_records, multi=True)
        history_records_1 = mycursor.fetchall()
        cnx.close()
        print(history_records_1)

    try:
        cnx2 = mysql.connector.connect(database=database2, user=user_database2, host=host_database2, password=password_database2)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database {} does not exist".format(database1))
        else:
            print(err)
    else:
        mycursor2 = cnx2.cursor()
        mycursor2.execute(select_some_records, multi=True)
        history_records_2 = mycursor2.fetchall()
        cnx2.close()
        print(history_records_2)


parser = argparse.ArgumentParser(description="Provide database details")
parser.add_argument("--db1", dest="database1", type=str,
                    help="Please provide name of first db", required=True)
parser.add_argument("--user_db1", dest="user_database1", type=str,
                    help="Please provide name of user to first db", required=True)
parser.add_argument("--host_db1", dest="host_database1", type=str,
                    help="Please provide hostname or address of first db", required=True)
parser.add_argument("--pass_db1", dest="password_database1", type=str,
                    help="Please provide password to db", required=False)
parser.add_argument("--db2", dest="database2", type=str,
                    help="Please provide name of second db", required=True)
parser.add_argument("--user_db2", dest="user_database2", type=str,
                    help="Please provide name of user to second db", required=True)
parser.add_argument("--host_db2", dest="host_database2", type=str,
                    help="Please provide hostname or address of second db", required=True)
parser.add_argument("--pass_db2", dest="password_database2", type=str,
                    help="Please provide password to db", required=False)

args = parser.parse_args()
database1 = args.database1
database2 = args.database2
host_database1 = args.host_database1
host_database2 = args.host_database2
user_database1 = args.user_database1
user_database2 = args.user_database2
password_database1 = args.password_database1
password_database2 = args.password_database2

if __name__ == '__main__':
    select_records(database1, user_database1, host_database1, password_database1, database2, user_database2, host_database2, password_database2)