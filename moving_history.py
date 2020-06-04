import mysql.connector
from mysql.connector import errorcode


select_history_records = """
SELECT * FROM history
WHERE date_created < now() - interval 365 DAY
ORDER BY date_created ASC LIMIT 1"""

insert_history_records = """
INSERT INTO history (history_id, person_id, tablename, record_id, related_tablename, related_record_id, columnname, value_from, val    ue_to, action_type, date_created, changes)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

compare_records = """
SELECT * FROM history
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
        history_records2 = mycursor3.fetchall()
        cnx3.close()
        return history_records2

    # TODO compare records from first db with inserted records from the second db


if compare_records from bfc_crm_backup_2 in:
    # how to return history_id and person_id from first select
    wynik = result.fetchall()
    wynik2 = [lis[:2] for lis in wynik]
    print(wynik[0])

    delete_records from bfc_crm_backup
