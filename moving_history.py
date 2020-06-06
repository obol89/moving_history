import mysql.connector
from mysql.connector import errorcode
import argparse


select_history_records = """
SELECT * FROM history
WHERE date_created < now() - interval 365 DAY
ORDER BY date_created ASC LIMIT 1"""

insert_history_records = """
INSERT INTO history (history_id, person_id, tablename, record_id, related_tablename, related_record_id, columnname, value_from, val    ue_to, action_type, date_created, changes)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

compare_history_records = """
SELECT * FROM history
WHERE history_id = %s and person_id = %s"""

delete_history_records = """
DELETE FROM history
WHERE history_id = %s and person_id = %s"""


def select_records():
    try:
        cnx = mysql.connector.connect(database="bfc_crm_backup", user='root', host='127.0.0.1')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        mycursor = cnx.cursor()
        mycursor.execute(select_history_records, multi=True)
        history_records = mycursor.fetchall()
        cnx.close()
        return history_records


def insert_records():
    try:
        cnx2 = mysql.connector.connect(database="bfc_crm_backup_2", user='root', host='127.0.0.1')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        mycursor2 = cnx2.cursor()
        mycursor2.executemany(insert_history_records, select_records())
        cnx2.commit()
        cnx2.close()
        return mycursor2.rowcount


def delete_records():
    # Connection to second DB
    try:
        cnx3 = mysql.connector.connect(database="bfc_crm_backup_2", user='root', host='127.0.0.1')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        mycursor3 = cnx3.cursor()
        mycursor3.execute(select_history_records, multi=True)
        history_records3 = mycursor3.fetchall()
        cnx3.close()
        history_records_3_1 = [lis[:2] for lis in history_records3]

    # Connection to first DB
    try:
        cnx4 = mysql.connector.connect(database="bfc_crm_backup", user='root', host='127.0.0.1')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        mycursor4 = cnx4.cursor()
        mycursor4.execute(select_history_records, multi=True)
        history_records4 = mycursor4.fetchall()
        history_records_4_1 = [lis[:2] for lis in history_records4]
        cnx4.close()

    # Deleting records from first DB
    if history_records_4_1[0] == history_records_3_1[0]:
        try:
            cnx5 = mysql.connector.connect(database="bfc_crm_backup", user='root', host='127.0.0.1')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your username or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            mycursor5 = cnx5.cursor()
            mycursor5.execute(delete_history_records, history_records_3_1[0], multi=True)
            cnx5.commit()
            cnx5.close()
            return "{} have been deleted".format(history_records_3_1)
    else:
        return "Sorry record wasn't found in the first DB"


def main():
    parser = argparse.ArgumentParser(description="Provide database details")
    parser.add_argument("--db1", dest="database1", type=str,
                        help="Please provide name of first db", required=True)
    parser.add_argument("--user_db1", dest="user_database1", type=str,
                        help="Please provide name of user to first db", required=True)
    parser.add_argument("--host_db1", dest="host_database1", type=str,
                        help="Please provide hostname or address of first db", required=True)
    parser.add_argument("--db2", dest="database2", type=str,
                        help="Please provide name of second db", required=True)
    parser.add_argument("--user_db2", dest="user_database2", type=str,
                        help="Please provide name of user to second db", required=True)
    parser.add_argument("--host_db2", dest="host_database2", type=str,
                        help="Please provide hostname or address of second db", required=True)
