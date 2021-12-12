import pymysql
import subprocess

class MySqlConnection:
    def __init__(self, db_host, db_user, db_pass, db_name):
        self.db_host = db_host
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name

    def connectDB(self):
        # Connect to the database
        connection = pymysql.connect(host=self.db_host,  
                                     user=self.db_user,
                                     password=self.db_pass,
                                     database=self.db_name,
                                     cursorclass=pymysql.cursors.DictCursor)
        return connection

    def cloneDB(self, database_name):
        # create DB
        connection = self.connectDB()
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE `{database_name}`;")

        # copy DB
        subprocess.Popen(f"mysqldump -u {self.db_user} --password={self.db_pass} {self.db_name} | mysql -u {self.db_user} -p {database_name}", shell=True)
        
        return MySqlConnection(self.db_host, self.db_user, self.db_pass, database_name)


def group_by_where():
    pass

def get_update_table():
    pass

def anonymize_database(strategy):
    # call get_update_table

    # apply that change on the database
    pass


# connection = MySqlConnection('localhost', 'root', 'charizard', 'feedback')
# new_connection = connection.cloneDB('feedback2')

'''
Output: Copy of database with specified tables anonymized through data masking
TODO:
- Connect to database with pymysql
- Create copy of database
- Update/Anonymize specified table (build and perform update query on copy_table)

Then in demo show aggregation functions produce same result (distribution remains the same)
'''
