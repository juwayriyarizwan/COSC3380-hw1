import sys
# Import the psycopg2 (Python driver that connects to psql)
import psycopg2
# Import your psql credentials from the python file db_config.py
from db_config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

# Initialize cursor and connection objects
# The "cursor" allows you to execute querys in psql and store its results.
# The "connection" authenticates the imported credentials from db_config.py to establish a connection with the "cursor" to psql.
cursor = None
connection = None

# On file run, checks if user entered data for a table
if (len(sys.argv) != 2):
    print ("Invalid input.")
    exit()

# Stores table information
tableInfo = sys.argv[1].split(";")
tableName = tableInfo[0].split("=",1)[1]
# Primary Key, can be composit
tablePk = tableInfo[1].split("=",1)[1].split(",")
# Table Columns
tableCol = tableInfo[2].split("=",1)[1].split(",")
joinPk = ','.join(tablePk)
joinCol = ','.join(tableCol)
currentForm = ""
validPk = False

#Checks if input is valid
if ("" in tablePk or "" in tableCol):
    print ("Invalid input.")
    exit()

try:
    # Establish a connection to the PostgreSQL database
    # Edit db_config.py to change the values that get imported
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    # Create a new database session and return a cursor object
    cursor = connection.cursor()

    # Open text file 
    sys.stdout = open("nf.txt", "a+")
    
    print("Database Connection Successful")
    
    # Checks if the table exists
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{tableName}');")
    if (cursor.fetchone()[0] != True):
        print("Table does not exist.")
        exit()
    
    # Checks if the primary key exists
    for pk in tablePk:
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='{tableName}' AND column_name='{pk}');")
        pkExists = cursor.fetchone()[0]
        if (pkExists != True):
            print(f"Primary key column {pk} does not exist.")
            exit()
    
    # Checks if the columns exists
    for col in tableCol:
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='{tableName}' AND column_name='{col}');")
        if (cursor.fetchone()[0] != True):
            print(f"Column {col} does not exist.")
            exit()
            
    print(tableName)
            
    # Validate the given primary key
    # Checks if there exists duplicate values in primary key
    cursor.execute(f"SELECT EXISTS (SELECT COUNT(*) FROM {tableName} GROUP BY {joinPk} HAVING COUNT(*) > 1);")
    checkDuplicate = cursor.fetchone()[0]
    if checkDuplicate == True:
        print(f"PK\tN") # There are duplicates
    else:
        print(f"PK\tY") # There are no duplicates
        validPk = True
        
    # 1NF Check
    # Checks if there exists duplicate rows
    cursor.execute(f"SELECT EXISTS (SELECT COUNT(*) FROM {tableName} GROUP BY {joinPk}, {joinCol} HAVING COUNT(*) > 1);")
    checkDuplicate = cursor.fetchone()[0]
    if checkDuplicate == True:
        print(f"1NF\tN") # There are duplicates
    else:
        print(f"1NF\tY") # There are no duplicates
        currentForm = "1NF"
        
    # 2NF Check
    # Checks for partial dependencies for composit keys
    if (validPk == True and len(tablePk) == 1 and currentForm == "1NF"):
        currentForm = "2NF" # 2NF if valid pk is not composit and table passes 1NF
    elif(validPk == True and currentForm == "1NF"):
        currentForm = "2NF"
        # For each attribute in a composit key, checks if duplicates exist
        for pk in tablePk:
            cursor.execute(f"SELECT EXISTS (SELECT COUNT(*) FROM {tableName} GROUP BY {pk} HAVING COUNT(*) > 1);")
            checkDuplicates = cursor.fetchone()[0]
            # if there are no duplicates, the primary key attribute is a partial dependency
            if checkDuplicates == False:
                currentForm = "1NF"
    if (currentForm == "2NF"):
        print(f"2NF\tY")
    else:
        print(f"2NF\tN")
    
    # Execute an SQL query to fetch data from table
    cursor.execute("SELECT * FROM " + tableName + ";")
    
    # Fetch all rows from the cursor into a list
    # rows = cursor.fetchall()
    # for row in rows:
        # print(row)
        # col1, col2 = row 
        # print(f"col1: {col1}, col2: {col2}")


# Print any errors that occured trying to establish connection or execute a query
except Exception as e:
    print(f"An error occurred: {e}")

# After the code succesfully executes make sure to close connections
# Connections can remain open if your program unexpectedly closes
finally:
    # Close the cursor to avoid memory leak
    if cursor:
        cursor.close()
    
    # Close the connection to free up resources
    if connection:
        connection.close()

# Close text file
sys.stdout.close()