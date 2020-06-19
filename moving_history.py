import mysql.connector
from mysql.connector import errorcode


select_history_records = """
SELECT * FROM history
WHERE date_created < now() - interval 365 DAY
ORDER BY date_created ASC LIMIT 1000"""

insert_history_records = """
INSERT INTO history (history_id, person_id, tablename, record_id, related_tablename, related_record_id, columnname, value_from, value_to, action_type, date_created, changes)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

delete_history_records = """
DELETE FROM history
WHERE history_id = %s and person_id = %s"""


def insert_records():
    try:
        cnx = mysql.connector.connect(database="mighty_crm", user="root", host="10.70.70.178", password="")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database mighty_crm does not exist")
        else:
            print(err)
    else:
        mycursor = cnx.cursor()
        mycursor.execute(select_history_records, multi=True)
        history_records_1 = mycursor.fetchall()
        cnx.close()

    try:
        cnx2 = mysql.connector.connect(database="mighty_crm_history", user="root", host="127.0.0.1", password="")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database mighty_crm_history does not exist")
        else:
            print(err)
    else:
        mycursor2 = cnx2.cursor()
        mycursor2.executemany(insert_history_records, history_records_1)
        cnx2.commit()
        cnx2.close()


def delete_records():
    try:
        cnx = mysql.connector.connect(database="mighty_crm", user="root", host="10.70.70.178", password="")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database mighty_crm does not exist")
        else:
            print(err)
    else:
        mycursor = cnx.cursor()
        mycursor.execute(select_history_records, multi=True)
        history_records_1 = mycursor.fetchall()
        cnx.close()

    try:
        cnx2 = mysql.connector.connect(database="mighty_crm_history", user="root", host="127.0.0.1", password="")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database mighty_crm_history does not exist")
        else:
            print(err)
    else:
        mycursor2 = cnx2.cursor()
        mycursor2.execute(select_history_records, multi=True)
        history_records_2 = mycursor2.fetchall()
        cnx2.close()

    history_rec_id_1 = [lis[:2] for lis in history_records_1]
    history_rec_id_2 = [lis[:2] for lis in history_records_2]
    records_to_delete = []

    for i in history_rec_id_2:
        if i in history_rec_id_1:
            records_to_delete.append(i)
        else:
            continue

    try:
        cnx5 = mysql.connector.connect(database="mighty_crm", user="root", host="10.70.70.178", password="")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        mycursor5 = cnx5.cursor()
        mycursor5.executemany(delete_history_records, history_rec_id_1)
        cnx5.commit()
        cnx5.close()
        print(mycursor5.rowcount, "records deleted")


if __name__ == '__main__':
    insert_records()
    delete_records()
