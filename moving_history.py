import mysql.connector
from mysql.connector import errorcode


class MovingHistory:


    def __init__(self, db_host, db_user, db_password, db_database):
        self.db_host     = db_host
        self.db_user     = db_user
        self.db_password = db_password
        self.db_database = db_database

    def get_history_records(self):
        select_history_records = """
        SELECT * FROM history
        WHERE date_created = '%s'"""
        date_created = ""

        try:
            cnx = mysql.connector.connect(host="localhost",
                                          database="bfc_crm_backup")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your username or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            mycursor = cnx.cursor()
            mycursor.execute(select_history_records, date_created)
            history_records = mycursor.fetchall()
            cnx.close()
            return history_records


        with mysql.connector.connect(host="localhost", database="bfc_crm_backup") as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_history_records, date_created)
#
#mydb2 = mysql.connector.connect(
#    host = "localhost",
#    # user = "username",
#    # passwd = "password",
#    database = "bfc_crm_backup_2"
#)
#
#insert_into_dst_database = """
#INSERT INTO history 'columns'
#SELECT 'columns'
#FROM history
#WHERE date_created = '%s'"""
#
#delete_from_src_database = """
#DELETE FROM history
#WHERE date_created = '%s'"""
#
#mycursor = mydb.cursor()
#mycursor.execute(query1)
#myresult = mycursor.fetchall()
#
#for x in myresult:
#    print(x)
#
#mycursor2 = mydb2.cursor()
#mycursor2.execute(query2)
#myresult2 = mycursor2.fetchall()
#
#for x in myresult2:
#    print(x)
#
#
## TODO: https://dba.stackexchange.com/questions/110557/copy-tables-from-one-database-to-another
#
#
#if select_history_records in mydb2:
#    delete select_history_records from mydb1
#
#
#
#
