from threading import Thread, Lock
import pymongo
import time
import timeit
import csv
import time
mutex = Lock()

def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData

def createDefaultTable():
    
    # Sem argumentos, ele conecta ao localhost no port 27017
    createDb = client["test"]
    createCollection = createDb["images"]
    return client.test, client.test.images
    
    print("Could not create")

def deleteTable():
    try:
        client.test.images.drop()
        client.drop_database('test')
    except:
        print("Could not drop")

def insertExecute(image, n):
    json = {
        "image": image,
        "metadata": n
    }
    collection.insert_one(json)

def insertMultiple(images, n):
    json = [
        {
        "image": images[0],
        "metadata": n
        },
        {
        "image": images[1],
        "metadata": n
        },
        {
        "image": images[2],
        "metadata": n
        },
    ]
    collection.insert_many(json)

def readExecute(n):
    image = collection.find_one({"metadata": n})
#    if image: print(image)
#    else: print('non')
#    time.sleep(3)
#    image = collection.find_one()
#    print(image)
#    time.sleep(10)

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
    with open('dataMultipleMongoSelect.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertBurst(n):
    data = []
    file = open("./image/imageset/metadata/descendingOrder.txt", "r")

    for num in range(20):
        images = []
        x=0
        extension1=".JPEG"
        extension2=".jpg"
        db, collection = createDefaultTable()
        if(n > 0):
            while x < n:
                images=[]
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
                if(num == 19):
                    data.append([x, time])
                print('Number: %s', x)
        deleteTable()
        file.seek(0,0)

    header = ['Number', 'Time']

    with open('dataBurstDescendingMongo.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def deleteExecute(n):
#    readExecute(n)
    res =collection.delete_one({"metadata": n})
#    readExecute(n)
#    print(res.acknowledged)
#    print("Deleted:", res.deleted_count)
#    time.sleep(10)

def deleteTest(n):
    data = []
    print("Deleting...")
    for x in range(n):
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: deleteExecute(x), number=1)
        finally:
            mutex.release()
        data.append([x,time])

    header = ['Number', 'Time']
    with open('dataRandomMongoDelete.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertIndividual(n):
    data = []
    file = open("./image/imageset/metadata/fileMultiple.txt", "r")

    for num in range(20):
        extension1=".JPEG"
        extension2=".jpg"
        if(num == 19):
            data = []
        db, collection = createDefaultTable()
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
                    time = timeit.timeit(lambda: insertExecute(image, x), number=1)
                finally:
                    mutex.release()
                if(num == 19):
                    data.append([x, time])
                print('Number: %s', x)
        readTest(n)
        #deleteTest(n)
        return
        deleteTable()
        file.seek(0,0)

    header = ['Number', 'Time']

    with open('dataDescendingMongo.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")


client = pymongo.MongoClient()
deleteTable()
db, collection = createDefaultTable()
t = Thread(target = insertIndividual, args=[3300])
t.start()


