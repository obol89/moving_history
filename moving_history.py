import mysql.connector
from mysql.connector import errorcode
import datetime


select_history_records = """
SELECT * FROM history
WHERE date_created < now() - interval 365 DAY
ORDER BY date_created ASC LIMIT 1"""

insert_history_records = """
INSERT INTO history (history_id, person_id, tablename, record_id, related_tablename, related_record_id, columnname, value_from, value_to, action_type, date_created, changes)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

compare_records = """
SELECT * FROM history
WHERE history_id = %s and person_id = %s"""


# def daterange(date1, date2):
#     for n in range(int((date2 - date1).days)+1):
#         yield date1 + datetime.timedelta(n)


# start_dt = datetime.datetime.now() - datetime.timedelta(days=365)
# end_dt = datetime.datetime.now()
# date_created = []
# for dt in daterange(start_dt, end_dt):
#     date_created.append(dt.strftime("%Y-%m-%d"))

# print(', '.join(date_created))

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
        for result in mycursor.execute(select_history_records, multi=True):
            if result.with_rows:
                # print("Rows produced by statement '{}':".format(result.statement))
                print(result.fetchall())
            else:
                # print("Number of rows affected by statement '{}': {}".format(result.statement, result.rowcount))
        cnx.close()


def insert_records():
    insert_stmt = (
        "INSERT INTO employees (emp_no, first_name, last_name, hire_date) "
        "VALUES (%s, %s, %s, %s)"
        )
    data = (2, 'Jane', 'Doe', datetime.date(2012, 3, 23))
    cursor.execute(insert_stmt, data)

    select_stmt = "SELECT * FROM employees WHERE emp_no = %(emp_no)s"
    cursor.execute(select_stmt, { 'emp_no': 2 })


# how to return history_id and person_id from first select
wynik = result.fetchall()
wynik2 = [lis[:2] for lis in wynik]
print(wynik[0])
