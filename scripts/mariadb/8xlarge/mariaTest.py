from threading import Thread, Lock
import mariadb
import timeit
import csv
mutex = Lock()
import time

def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData

def handleDatabase():
    cursor.execute("DROP DATABASE IF EXISTS test")
    cursor.execute("CREATE DATABASE IF NOT EXISTS test")
    cursor.execute("USE test")

def createDefaultTable():
    try:
        cursor.execute('CREATE TABLE images(id int NOT NULL AUTO_INCREMENT, image MEDIUMBLOB, metadata VARCHAR(255), PRIMARY KEY (id))')
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

def insertMultiple(images, n):
    query = "INSERT INTO images (image, metadata) VALUES (?, ?), (?, ?), (?, ?)" 
    cursor.execute(query, (images[0], n, images[1], n, images[2], n))
    conn.commit()

def readExecute(n):
    cursor.execute('SELECT image FROM images WHERE id=?', (n,))
    data=cursor.fetchone()
    if data:
        print(len(data))
    print('a')

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
    with open('dataSelectMaria.csv', 'w', newline='') as f:
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
        readTest(cursor, conn, n)
        return
        deleteTable()
        file.seek(0, 0)

    cursor.close()
    conn.close()

    header = ['Number', 'Time']


    with open('dataBurstDescendingMaria.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def deleteExecute(n):
#    readExecute(n)
    cursor.execute(f'DELETE FROM images WHERE id=\'{n}\'')
#    readExecute(n)
    conn.commit()

def deleteTest(n):
    data = []
#    time.sleep(50)
    for x in range(n):
        mutex.acquire()
        try:
            timex = timeit.timeit(lambda: deleteExecute(x+1), number=1)
        finally:
            mutex.release()
        data.append([x,timex])
        print(timex)

    header = ['Number', 'Time']
    with open('dataDescendingMariaDelete.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

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
                image = convertToBinaryData("./image/imageset/" + imFile[0] + extension)
                mutex.acquire()
                try:
                    time = timeit.timeit(lambda: cursorExecute(image, x), number=1)
                finally:
                    mutex.release()
                if(num==19):
                    data.append([x, time])
                print('Number: %s', x)
        deleteTest(n)
        return
        deleteTable()
        file.seek(0, 0)
    cursor.close()
    conn.close()

    header = ['Number', 'Time']

    with open('dataDescendingMaria.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")


try:
    conn = mariadb.connect(
        user="root",
        password="password",
        host="localhost",
        port=3306
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

cursor = conn.cursor()

handleDatabase()
deleteTable()
createDefaultTable()

t = Thread(target = insertIndividual, args=[3300])
t.start()
