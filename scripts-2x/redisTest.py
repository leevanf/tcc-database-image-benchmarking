from threading import Thread, Lock
import redis
import uuid
import timeit
import csv
mutex = Lock()

def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData

def createDefaultTable():
    #Not necessary for redis
    print("Could not create")

def deleteTable():
    try:
        keys = client.keys('*')
        client.delete(*keys)
    except:
        print("Could not drop")

def multi_push(images, n, filename):
    pipe = client.pipeline()
    for val in images:
        pipe.hset(filename, mapping={
        'image': val,
        'metadata': n
        })
    pipe.execute()

def insertExecute(image, n, filename):
    #client.set(filename, image)
    client.hset(filename, mapping={
        'image': image,
        'metadata': n
    })

def insertBurst(n):
    data = []
    file = open("./image/imageset/metadata/descendingOrder.txt", "r")

    for num in range(20):
        images = []
        x=0
        extension1=".JPEG"
        extension2=".jpg"
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
                    time = timeit.timeit(lambda: multi_push(images, n, imFile[0]), number=1)
                finally:
                    mutex.release()
                if(num == 19):
                    data.append([x, time])
                print('Number: %s', x)
        deleteTable()
        file.seek(0,0)

    header = ['Number', 'Time']

    with open('dataBurstDescendingRedis.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def deleteExecute(filename):
    client.hdel(filename, "image")

def deleteTest(n):
    file = open("./image/imageset/metadata/fileMultiple.txt", "r")
    data = []
    for x in range(n):
        imFile = file.readline().split(' ')
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: deleteExecute(imFile[0]), number=1)
        finally:
            mutex.release()
        data.append([x,time])

    header = ['Number', 'Time']
    with open('dataDescendingRedisDelete.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def readExecute(filename):
    image = client.hget(filename, "image")

def readTest(n):
    file = open("./image/imageset/metadata/fileMultiple.txt", "r")
    data = []
    for x in range(n):
        imFile = file.readline().split(' ')
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: readExecute(imFile[0]), number=1)
        finally:
            mutex.release()
        data.append([x,time])

    header = ['Number', 'Time']
    with open('dataSelectRedis.csv', 'w', newline='') as f:
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
        if(n > 0):
            for x in range(n):
                imFile = file.readline().split(' ')
                if(imFile[0][0] == 'I'):
                    extension=extension1
                else:
                    extension=extension2
                image = convertToBinaryData("./image/imageset/" + imFile[0] + extension)
                filename = imFile[0]
                mutex.acquire()
                try:
                    time = timeit.timeit(lambda: insertExecute(image, n, filename), number=1)
                finally:
                    mutex.release()
                if(num == 19):
                    data.append([x, time])
                print('Number: %s', x)
        deleteTest(n)
#        readTest(n)
        return
        deleteTable()
        file.seek(0,0)

    header = ['Number', 'Time']

    with open('dataMultipleRedis.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")


client = redis.Redis(host='localhost', port=6379, decode_responses=False)
deleteTable()

t = Thread(target = insertIndividual, args=[3300])
t.start()
