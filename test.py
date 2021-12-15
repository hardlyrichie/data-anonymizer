from collections import defaultdict

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

print(table_and_cols)

# db_pass = ''
# with open('password.txt', 'r') as f:
#     db_pass = f.read()
# conn = MySqlConnection('localhost', 'root', db_pass, db_name)
# # Creates clone to apply data anonymization on. 
# # If database already exits, user should drop it first. Currently does not check if databse
# # with same name already exists
# anon = conn.cloneDB(f'{db_name}_anonymized')

# table_and_cols