import pymysql
import subprocess
import numpy as np
from faker import Faker
from collections import defaultdict

class MySqlConnection:
    def __init__(self, db_host, db_user, db_pass, db_name):
        self.db_host = db_host
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name

        self.faker = Faker()
        Faker.seed(0)

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

    def randomizeCategorical(self, table_name, column_name, faker_provider, where_clause=None):
        print('Performing categorical anonymization for', table_name, column_name)
        connection = self.connectDB()
        with connection.cursor() as cursor:
            ## get data type of the original column
            # cursor.execute(f"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{column_name}';")
            # data_type = cursor.fetchone()['DATA_TYPE']

            ## find the number of distinct values of a given column
            # cursor.execute(f'SELECT COUNT(DISTINCT {column_name}) FROM {table_name};')
            # n = cursor.fetchone()['COUNT(DISTINCT name)']

            # get all distinct values of a given column
            cursor.execute(f'SELECT DISTINCT {column_name} FROM {table_name} {"WHERE " + where_clause if where_clause else ""};')
            result = cursor.fetchall()
            all_values = []
            for value in result:
                all_values.append(value[column_name])

            # generate fake categorical values
            fake_values = []
            for i in range(len(all_values)):
                fake_values.append(faker_provider())
                cursor.execute(f'UPDATE {table_name} SET {column_name} = REPLACE({column_name}, "{all_values[i]}", "{fake_values[i]}")')
        
        # commit changes
        connection.commit()
        
    def randomizeNumerical(self, table_name, column_name, preserve_distr=False):
        print('Performing numerical anonymization for', table_name, column_name)
        connection = self.connectDB()
        with connection.cursor() as cursor:
            ## just generate one value and apply it everywhere
            if preserve_distr:
                fake_val = np.random.uniform(0, 1.1)

                # get all distinct values of a given column
                cursor.execute(f'SELECT DISTINCT {column_name} FROM {table_name};')
                result = cursor.fetchall()
                print(result)
                all_values = []
                for value in result:
                    all_values.append(value[column_name])

                # apply fake_val
                for i in range(len(all_values)):
                    cursor.execute(f'UPDATE {table_name} SET {column_name} = REPLACE({column_name}, "{all_values[i]}", "{all_values[i] + 10 * fake_val}")')
                
            ## generate different values for each distinct value
            else:
                # get all distinct values of a given column
                cursor.execute(f'SELECT DISTINCT {column_name} FROM {table_name};')
                result = cursor.fetchall()
                all_values = []
                for value in result:
                    all_values.append(value[column_name])

                # apply fake_val
                for i in range(len(all_values)):
                    cursor.execute(f'UPDATE {table_name} SET {column_name} = REPLACE({column_name}, "{all_values[i]}", "{all_values[i] + 10 * np.random.uniform(0, 1.1)}")')


        # commit changes
        connection.commit()
    
    def getProvider(self, name):
        if name == 'person':
            return self.faker.name
        if name == 'company':
            return self.faker.company
        if name == 'date_time':
            return self.faker.date

if __name__ == "__main__":
    # TODO: Put copying database step in python multiprocessing queue so 
    # that we can wait for it to finish before proceeding to transformations
    lines = []
    # Read from instruction file
    with open('anonymize_instructions.txt', 'r') as f:
        lines = f.readlines()
    
    # Get database, table, and columns to be anonymized
    table_and_cols = defaultdict(list) # key: table name, value: list of columns in table
    if len(lines) > 1:
        lines = [line.rstrip() for line in lines]
        db_name = lines[0]
        for i in range(1, len(lines)):
            names = lines[i].split(', ')
            if len(names) > 1:
                for cols in names[1:]:
                    table_and_cols[names[0]].append(tuple(cols.split()))
    else:
        raise ValueError('anonymize_instructions.txt is empty. Use this file to detail what to anonymize')

    db_pass = ''
    with open('password.txt', 'r') as f:
        db_pass = f.read()
    conn = MySqlConnection('localhost', 'root', db_pass, db_name)
    # Creates clone to apply data anonymization on. 
    # If database already exits, user should drop it first. Currently does not check if databse
    # with same name already exists
    anon = conn.cloneDB(f'{db_name}_anonymized')

    # --------------
    # lines = []
    # # Read from instruction file
    # with open('anonymize_instructions.txt', 'r') as f:
    #     lines = f.readlines()
    
    # # Get database, table, and columns to be anonymized
    # table_and_cols = defaultdict(list) # key: table name, value: list of columns in table
    # if len(lines) > 1:
    #     lines = [line.rstrip() for line in lines]
    #     db_name = lines[0]
    #     for i in range(1, len(lines)):
    #         names = lines[i].split(', ')
    #         if len(names) > 1:
    #             for cols in names[1:]:
    #                 table_and_cols[names[0]].append(tuple(cols.split()))
    # else:
    #     raise ValueError('anonymize_instructions.txt is empty. Use this file to detail what to anonymize')

    # db_pass = ''
    # with open('password.txt', 'r') as f:
    #     db_pass = f.read()

    # anon = MySqlConnection('localhost', 'root', db_pass, f'{db_name}_anonymized')

    # for table in table_and_cols:
    #     for col in table_and_cols[table]:
    #         if col[1] == 'categorical':
    #             anon.randomizeCategorical(table, col[0], anon.getProvider(col[2]))
    #         elif col[1] == 'numerical':
    #             anon.randomizeNumerical(table, col[0])
    #         else:
    #             raise ValueError('Unknown column type. Must either be categorical or numerical')

            
    
