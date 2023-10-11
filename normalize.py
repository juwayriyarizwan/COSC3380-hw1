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
# Primary Key, can be composite
tablePk = tableInfo[1].split("=",1)[1].split(",")
# Table Columns
tableCol = tableInfo[2].split("=",1)[1].split(",")

emptyCol = False
joinPk = ','.join(tablePk)
joinCol = ','.join(tableCol)
joinAll = ""
# Checks if columns are empty and joins primary key and columns into comma separated string
if tableCol[0] == '':
    emptyCol = True
    joinAll = joinPk
else:
    joinAll = joinPk + "," + joinCol

currentForm = ""
validPk = False

# Checks if input is valid
if ("" in tablePk):
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
    
    print("Database Connection Successful")

    # Checks if the table exists
    # cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{tableName}');")
    # if cursor.fetchone()[0] == False:
    #     print("Invalid input.")
    #     exit()

    # # Checks if the primary key exists
    # for pk in tablePk:
    #     cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='{tableName}' AND column_name='{pk}');")
    #     pkExists = cursor.fetchone()[0]
    #     if pkExists == False:
    #         print(f"Invalid input.")
    #         exit()

    # # Checks if non-empty columns exist
    # if emptyCol == False:
    #     for col in tableCol:
    #         cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='{tableName}' AND column_name='{col}');")
    #         if cursor.fetchone()[0] == False:
    #             print(f"Invalid input.")
    #             exit()
            
    # Open text file 
    sys.stdout = open("nf.txt", "a+")  
    
    
            
    # Validate the given primary key
    cursor.execute(f"SELECT EXISTS (SELECT COUNT(*) FROM db.{tableName} GROUP BY {joinPk} HAVING COUNT(*) > 1);")
    checkDuplicate = cursor.fetchone()[0]
    # Checks if there exists duplicate values in primary key with 1 attribute
    if len(tablePk) == 1:
        if checkDuplicate == True:
            print(f"PK\tinvalid") # There are duplicates
        else:
            print(f"PK\tvalid") # There are no duplicates
            validPk = True
    else:
        # For each individual attribute in a composite key, checks if duplicates exist
        validPk = True
        for pk in tablePk:
            cursor.execute(f"SELECT EXISTS (SELECT COUNT(*) FROM db.{tableName} GROUP BY {pk} HAVING COUNT(*) > 1);")
            checkDuplicates = cursor.fetchone()[0]
            # If there are no duplicates, the primary key attribute is a partial dependency
            if checkDuplicates == False:
                validPk = False

        # Checks if full composite key has duplicates
        cursor.execute(f"SELECT EXISTS (SELECT COUNT(*) FROM db.{tableName} GROUP BY {joinPk} HAVING COUNT(*) > 1);")
        checkDuplicate = cursor.fetchone()[0]
        if checkDuplicate == True:
            validPk = False # There are no duplicates
        
        if validPk == True:
            print(f"PK\tvalid") # Attributes fully dependant on full key
        else:
            print(f"PK\tinvalid") # Partial dependency, not minimal

        
    # 1NF Check
    # Checks if there exists duplicate rows
    cursor.execute(f"SELECT EXISTS (SELECT COUNT(*) FROM db.{tableName} GROUP BY {joinAll} HAVING COUNT(*) > 1);")
    checkDuplicate = cursor.fetchone()[0]
    if checkDuplicate == True:
        print(f"1NF\tN") # There are duplicates
    else:
        print(f"1NF\tY") # There are no duplicates
        currentForm = "1NF"
        
    # 2NF Check
    if validPk == True and currentForm == "1NF":
        currentForm = "2NF"
        print(f"2NF\tY")
    else:
        print(f"2NF\tN")
    
    # 3NF Check
    # Checks for dependencies from non-key attributes
    checkTransitiveDepend = False
    if currentForm == "2NF":
        for npk in set(tableCol) - set(tablePk):
            for col in tableCol:
                for pk in tablePk:
                    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {tableName} t1 WHERE NOT EXISTS (SELECT 1 FROM {tableName} t2 WHERE t2.{pk} = t1.{pk} AND t2.{col} <> t1.{col})) AS transitive_dependency;")
                    transitive_dependency = cursor.fetchone()[0]
                    if transitive_dependency:
                        checkTransitiveDepend = True
                        break
    if checkTransitiveDepend:
        print(f"3NF\t\tN")  # There are transitive dependencies 
    else:
        print(f"3NF\t\tY")  # There are no transitive dependencies
        currentForm = "3NF"

    # BCNF Check
    # Checks for dependencies from non-key attributes
    checkDependency = False
    if currentForm == "3NF":
        for npk in set(tableCol) - set(tablePk):
            for pk in tablePk:
                cursor.execute(f"SELECT EXISTS (SELECT COUNT(*) FROM {tableName} GROUP BY {pk}, {npk} HAVING COUNT(*) > 1);")
                if cursor.fetchone()[0]:
                    checkDependency = True
                    break
            if checkDependency:
                break 
    if checkDependency:
        print(f"BCNF\tN")  # There are transitive dependencies
    else:
        print(f"BCNF\tY")  # There are no transitive dependencies
        currentForm = "BCNF" 


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
