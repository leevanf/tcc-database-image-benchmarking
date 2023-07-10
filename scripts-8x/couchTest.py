from threading import Thread, Lock
import couchdb
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
    try:
        return couch.create("test")
    except:
        print("Could not create")
        return

def deleteTable():
    try:
        del couch['test']
    except:
        print("Could not drop")

def insertExecute(image, n):
    uuid_str=str(uuid.uuid4())
    image_str=str(image)
    json = {
        "id": uuid_str,
        "image": image_str,
        "metadata": n
    }
    db.save(json)

def insertIndividual(n):
    data = []
    file = open("./image/imageset/metadata/fileMultiple.txt", "r")

    for num in range(20):
        extension=".JPEG"
        createDefaultTable()
        if(n > 0):
            for x in range(n):
                if(x == 3010):
                    extension=".jpg"
                    print("Change extension")
                imFile = file.readline().split(' ')
                image = convertToBinaryData("./image/imageset/" + imFile[0] + extension)
                mutex.acquire()
                try:
                     time = timeit.timeit(lambda: insertExecute(image, n), number=1)
                finally:
                    mutex.release()
                if(num == 19):
                    data.append([x, time])
                print('Number: %s', x)

            deleteTable()
            file.seek(0, 0)


    header = ['Number', 'Time']

    with open('dataMultipleCouch.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")


couch = couchdb.Server("http://admin:password@localhost:5984")
# couch = couchdb.Server("http://localhost:5984")
deleteTable()
db = createDefaultTable()
t = Thread(target = insertIndividual, args=[3080])
t.start()
