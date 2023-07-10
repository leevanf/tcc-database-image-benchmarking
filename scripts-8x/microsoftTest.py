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

def quickRead(n):
    cursor.execute('SELECT image FROM images WHERE id=(?)', str(n))
    data = cursor.fetchone()
    if(data):
        print("foinf")
    else:
        print("none")

def deleteExecute(n):
#    quickRead(n)
    cursor.execute('DELETE FROM images WHERE id={}'.format(n))
    conn.commit()
#    quickRead(n)

def deleteTest(n):
    data = []
    for x in range(n):
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: deleteExecute(x+1), number=1)
        finally:
            mutex.release()
        data.append([x,time])

    header = ['Number', 'Time']
    with open('dataDescendingMicrosoftDelete.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertMultiple(images, n):
    values=[]
#    images = ['a', 'b', 'c']
    for image in images:
        values.append(f"(b'{image}', {n})")

    query = "INSERT INTO images (image, metadata) VALUES (?, ?), (?, ?), (?, ?)" #+ ', '.join(values)
    cursor.execute(query, (images[0], n, images[1], n, images[2], n))
    conn.commit()

def readExecute(n):
    cursor.execute('SELECT image FROM images WHERE id=(?)', str(n))
    data = cursor.fetchone()
    conn.commit()


def readTest(n):
    data = []
    for x in range(n):
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: readExecute(x), number=1)
        finally:
            mutex.release()
        data.append([x,time])

    header = ['Number', 'Time']
    with open('dataSelectMicrosoft.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertBurst(n):
    data = []
    file = open("./image/imageset/metadata/descendingOrder.txt", "r")

    for num in range(10):
        images = []
        x=0
        extension1=".JPEG"
        extension2=".jpg"
        createDefaultTable()
        if(n > 0):
            while x < n:
                images = []
                for y in range(3):
                    imFile = file.readline().split(' ')
                    if(imFile[0][0] == 'I'):
                        extension=extension1
                    else:
                        extension=extension2
                    images.append(convertToBinaryData("./image/imageset/" + imFile[0] + extension))
                    x+=1

                mutex.acquire()
                try:
                    time = timeit.timeit(lambda: insertMultiple(images, n), number=1)
                finally:
                    mutex.release()
                if(num == 9):
                    data.append([x, time])
                print('Number: %s', x)
#        readTest(cursor, conn, 100)
#        return
        deleteTable()
        file.seek(0, 0)

    cursor.close()
    conn.close()

    header = ['Number', 'Time']


    with open('dataBurstDescendingMicrosoft.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertIndividual(n):
    data = []
    file = open("./image/imageset/metadata/descendingOrder.txt", "r")

    for num in range(20):
        createDefaultTable()
        extension1=".JPEG"
        extension2=".jpg"
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
        deleteTest(n)
        return
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
