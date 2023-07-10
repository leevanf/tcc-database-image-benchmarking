from threading import Thread, Lock
import timeit
import csv
import uuid
from cassandra.cluster import Cluster
mutex = Lock()

def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData

def createDefaultTable():
    try:
        session.execute('CREATE TABLE images (id text, image blob, metadata text, PRIMARY KEY (id));')
        print("CREATED DEFAULT TABLE")
        return
    except:
        print("Could not create")

def deleteTable():
    try:
        session.execute('DROP TABLE IF EXISTS images')
    except:
        print("Could not drop")

def insertExecute(image, n):
    j=str(uuid.uuid4())
    session.execute(insertOnceStatement, (j, image, str(n)))

def insertMultiple(images, n):
    session.execute(insertOnceStatement, (str(uuid.uuid4()), images[0], str(n), str(uuid.uuid4()), images[1], str(n), str(uuid.uuid4()), images[2], str(n)))

def readExecute(n):
    data = session.execute(readStatement, (str(n),))

def readTest(n):
    data = []
    print("Reading..")
    for x in range(n):
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: readExecute(x+1), number=1)
        finally:
            mutex.release()
        print(x)
        data.append([x,time])

    header = ['Number', 'Time']
    with open('dataSelectCassandra.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertBurst(n):
    data = []
    file = open("./image/imageset/metadata/fileMultiple.txt", "r")

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
                    time = timeit.timeit(lambda: insertMultiple(images, x), number=1)
                finally:
                    mutex.release()
                if(num == 9):
                    data.append([x, time])
                print('Number: %s', x)
#        readTest(cursor, conn, 100)
#        return
        deleteTable()
        file.seek(0, 0)

    header = ['Number', 'Time']


    with open('dataBurstCassandra.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def getsid(n):
    data = session.execute(readIdStatement, (str(n),))
    if(data):
        for row in data:
            return row.id

def deleteExecute(n, deleteOnceStatement):
    stra=str(n)
    session.execute(deleteOnceStatement, (stra,))

def deleteTest(n):
    deleteOnceStatement = session.prepare("DELETE FROM images WHERE id=(?)")
    print("Deleting...")
    data = []
    for x in range(n):
        id=getsid(x+1)
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: deleteExecute(id, deleteOnceStatement), number=1)
        finally:
            mutex.release()
        data.append([x,time])

    header = ['Number', 'Time']
    with open('dataDescendingCassandraDelete.csv', 'w', newline='') as f:
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
                    print("Change extension")
                
                image = convertToBinaryData("./image/imageset/" + imFile[0] + extension)
                mutex.acquire()
                try:
                    time = timeit.timeit(lambda: insertExecute(image, x), number=1)
                finally:
                    mutex.release()
                if(num == 19):
                    data.append([x, time])
                print('Number: %s', x)

            deleteTest(n)
            return
            deleteTable()
            file.seek(0, 0)

    header = ['Number', 'Time']

    with open('dataMultipleCassandra.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")


cluster = Cluster(['172.17.0.6'],port=9042)
#! O keyspace precisa estar criado previamente
#! create keyspace test with replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
session = cluster.connect()
session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
session.execute("USE test;")
#! Estou usando um prepared statement para ser mais performÃ¡tico
deleteTable()
createDefaultTable()
readIdStatement= session.prepare("SELECT id FROM images WHERE metadata=? ALLOW FILTERING")
readStatement= session.prepare("SELECT image FROM images WHERE metadata=? ALLOW FILTERING")
insertOnceStatement = session.prepare("""
INSERT INTO images (id, image, metadata) VALUES (?, ?, ?);
""")
#INSERT INTO images (id, image, metadata) VALUES (?, ?, ?);
#INSERT INTO images (id, image, metadata) VALUES (?, ?, ?);
#APPLY BATCH
#""")
t = Thread(target = insertIndividual, args=[3300])
t.start()
