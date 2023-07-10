from threading import Thread, Lock
import pyodbc
import timeit
import csv
mutex = Lock()

def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData

def createDefaultTable():
    try:
        cursor.execute('CREATE TABLE images(id int IDENTITY(1,1) PRIMARY KEY, image VARBINARY(max), metadata VARCHAR(255))')
        conn.commit()
    except:
        print("Could not create")

def deleteTable():
    try:
        cursor.execute('DROP TABLE IF EXISTS images')
        conn.commit()
    except:
        print("Could not drop")

def cursorExecute(image, n):
    query = 'INSERT INTO images (image, metadata) VALUES (?, ?)'
    cursor.execute(query, (image, n))
    conn.commit()

def insertIndividual(n):
    data = []
    file = open("./image/imageset/metadata/descendingOrder.txt", "r")

    for num in range(20):
        extension1=".JPEG"
        extension2=".jpg"
        createDefaultTable()
        if(n > 0):
            for x in range(n):
                imFile = file.readline().split(' ')
                if(imFile[0][0] == 'I'):
                    extension=extension1
                else:
                    extension=extension2
                    print("Change extension")

                image = convertToBinaryData("./image/imageset/" + imFile[0] + extension)
                mutex.acquire()
                try:
                    time = timeit.timeit(lambda: cursorExecute(image, n), number=1)
                finally:
                    mutex.release()
                if(num == 19):
                    data.append([x, time])
                print('Number: %s', x)
        deleteTable()
        file.seek(0, 0)
    cursor.close()
    conn.close()

    header = ['Number', 'Time']

    with open('dataDescendingMicrosoftSQL.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

server = 'localhost' 
database = 'test' 
username = 'sa' 
password = '<Password123>' 
connectionString = 'DRIVER=ODBC Driver 17 for SQL Server;SERVER={};DATABASE={};UID={};PWD={};TrustServerCertificate=yes;'.format(server, database, username, password)

conn = pyodbc.connect(connectionString)
conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
conn.setencoding('latin1')
cursor = conn.cursor()

deleteTable()
createDefaultTable()

t = Thread(target = insertIndividual, args=[3300])
t.start()
