from threading import Thread, Lock
import psycopg2
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
        conn = psycopg2.connect(host="localhost", user="docker", password="docker", database="mydb", port="5432")
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE test (id SERIAL PRIMARY KEY, image BYTEA)')
        conn.commit()
        cursor.close()
        conn.close()
    except:
        print("Could not create")


def deleteTable():
    try:
        conn = psycopg2.connect(host="localhost", user="docker", password="docker", database="mydb", port="5432")
        cursor = conn.cursor()
        cursor.execute('DROP TABLE test')
        conn.commit()
        cursor.close()
        conn.close()
    except:
        print("Could not drop")


def cursorExecute(cursor, conn, image):
    cursor.execute('INSERT INTO test (image) VALUES (%s)', (image,))
    conn.commit()


def insertMultiple(cursor, conn, images):
    query = "INSERT INTO test (image) VALUES (%s), (%s), (%s)" 
    cursor.execute(query, (images[0], images[1], images[2]))
    conn.commit()

def readExecute(cursor, conn, n):
    cursor.execute('SELECT image FROM test WHERE id=%s', (str(n),))
    data=cursor.fetchone()
#    if(data):
#        print("found")
#    else:
#        print('nonw')

def readTest(cursor, conn, n):
    data = []
    for x in range(n):
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: readExecute(cursor, conn, x), number=1)
        finally:
            mutex.release()
        data.append([x,time])

    header = ['Number', 'Time']
    with open('dataSelectPostgres.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertBurst(n):
    data = []
    file = open("./image/imageset/metadata/descendingOrder.txt", "r")

    conn = psycopg2.connect(host="localhost", user="docker", password="docker", database="mydb", port="5432")
    cursor = conn.cursor()

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
                    time = timeit.timeit(lambda: insertMultiple(cursor, conn, images), number=1)
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


    with open('dataBurstDescendingPostgres.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def deleteExecute(cursor, conn, n):
#    readExecute(cursor, conn, n)
    cursor.execute(f'DELETE FROM test WHERE id=\'{n}\'')
    conn.commit()
#    readExecute(cursor, conn, n)

def deleteTest(cursor, conn, n):
    data = []
    print("Deleting....")
    for x in range(n):
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: deleteExecute(cursor, conn, x), number=1)
        finally:
            mutex.release()
        data.append([x,time])

    header = ['Number', 'Time']
    with open('dataDescendingPostgresDelete.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertIndividual(n):
    data = []
    file = open("./image/imageset/metadata/descendingOrder.txt", "r")

    conn = psycopg2.connect(host="localhost", user="docker", password="docker",  database="mydb", port="5432")
    cursor = conn.cursor()
    for y in range (10):
        extension1=".JPEG"
        extension2=".jpg"
        createDefaultTable()
        if (n > 0):
            for x in range(n):
                imFile = file.readline().split(' ')
                if(imFile[0][0] == 'I'):
                    extension=extension1
                else:
                    extension=extension2
                image = convertToBinaryData("./image/imageset/" + imFile[0] + extension)
                mutex.acquire()
                try:
                    time = timeit.timeit(lambda: cursorExecute(cursor, conn, image), number=1)
                finally:
                    mutex.release()
                if(y == 9): data.append([x, time])
                print('Number: %s', x)
            deleteTest(cursor, conn, n)
            return
            deleteTable()
            file.seek(0)
    cursor.close()
    conn.close()

    header = ['Number', 'Time']

    with open('dataDescendingPostgres.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")


deleteTable()
createDefaultTable()
t = Thread(target=insertIndividual, args=[3300])
t.start()
