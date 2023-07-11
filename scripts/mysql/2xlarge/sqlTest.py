from threading import Thread, Lock
import pymysql
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
        conn = pymysql.connect(host="localhost", user="testuser", password="password", database="mytest")
        cursor = conn. cursor()
        cursor.execute('CREATE TABLE test(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, image MEDIUMBLOB)')
        conn.commit()
        cursor.close()
        conn.close()
    except:
        print("Could not create")

def deleteTable():
    try:
        conn = pymysql.connect(host="localhost", user="testuser", password="password", database="mytest")
        cursor = conn. cursor()
        cursor.execute('DROP TABLE test')
        conn.commit()
        cursor.close()
        conn.close()
    except:
        print("Could not drop")

def cursorExecute(cursor, conn, image):
    cursor.execute('INSERT INTO test (image) VALUES (%s)', image)
    conn.commit()

def insertMultiple(cursor, conn, images):
    cursor.executemany('INSERT INTO test (image) VALUES (%s)', [images[0], images[1], images[2]])
    conn.commit()

def readExecute(cursor, conn, n):
    cursor.execute('SELECT image FROM test WHERE id=%s', str(n))
    data=cursor.fetchone()

def deleteExecute(cursor, conn, n):
    cursor.execute(f'DELETE FROM test WHERE id=\'{n}\'')
    conn.commit()

def deleteTest(cursor, conn, n):
    data = []
    for x in range(n):
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: deleteExecute(cursor, conn, x), number=1)
        finally:
            mutex.release()
        data.append([x,time])
        print(time)

    header = ['Number', 'Time']
    with open('dataDescendingMysqlDelete.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")


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
    with open('dataSelectMysql.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertBurst(n):
    data = []
    file = open("./image/imageset/metadata/descendingOrder.txt", "r")

    conn = pymysql.connect(host="localhost", user="testuser", password="password", database="mytest")
    cursor = conn.cursor()

    for num in range(3):
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
                if(num == 2):
                    data.append([x, time])
                print('Number: %s', x)

        deleteTable()
        file.seek(0, 0)

    cursor.close()
    conn.close()

    header = ['Number', 'Time']


    with open('dataBurstDescendingMysql.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertIndividual(n):
    data = []
    file = open("./image/imageset/metadata/descendingOrder.txt", "r")

    conn = pymysql.connect(host="localhost", user="testuser", password="password", database="mytest")
    cursor = conn.cursor()

    for num in range(10):
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
                    time = timeit.timeit(lambda: cursorExecute(cursor, conn, image), number=1)
                finally:
                    mutex.release()
                if(num == 9):
                    data.append([x, time])
                print('Number: %s', x)
        deleteTest(cursor, conn, n)
        return
        deleteTable()
        file.seek(0, 0)
    cursor.close()
    conn.close()

    header = ['Number', 'Time']

    with open('dataDescendingMysql.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertData(n):
    mutex.acquire()
    data = []
    try:
        image = convertToBinaryData("oB5B5tU.png")
        conn = pymysql.connect(host="localhost", user="testuser", password="password", database="mytest")
        cursor = conn.cursor()
        if(n > 0):
            for x in range(n):
                time = timeit.timeit(lambda: cursorExecute(cursor, conn, image), number = 1)
                data.append([x, time])
                print('Number: %s', x)
            cursor.close()
            conn.close()
        """
        db = pymysql.connect(host="localhost",user='user',password='password',database='MYSQLTEST', unix_socket="True")
        cursor = db.cursor()
        cursor.execute('INSERT INTO test_table (image) VALUES ({0})'.format(image))
        db.commit()
        cursor.close()
        db.close()
        """
    finally:
        mutex.release()
        header = ['Number', 'Time']
        
        with open('dataMultiple.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(data)
            print("Writing line")

deleteTable()
t = Thread(target = insertIndividual, args=[3300])
t.start()

