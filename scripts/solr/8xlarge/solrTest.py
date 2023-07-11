from threading import Thread, Lock
import pysolr
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
    #Not necessary for Solr
    print("Could not create")

def deleteTable():
    try:
        solr.delete(q='*:*')
    except:
        print("Could not drop")

def insertExecute(image, n):
    uuid_txt = str(uuid.uuid4())
    image_txt = str(image)
    solr.add({
        "id": uuid_txt,
        "image": image_txt,
        "metadata": n
    })

def insertMultiple(images, n):
    uuid_txt1 = str(uuid.uuid4())
    uuid_txt2 = str(uuid.uuid4())
    uuid_txt3 = str(uuid.uuid4())
    image1_txt = str(images[0])
    image2_txt = str(images[1])
    image3_txt = str(images[2])

    solr.add([{
        "id": uuid_txt1,
        "image": image1_txt,
        "metadata": n
    },
    {
        "id": uuid_txt1,
        "image": image2_txt,
        "metadata": n
    },
    {
        "id": uuid_txt1,
        "image": image3_txt,
        "metadata": n
    }])


def readExecute(n):
    query = 'metadata: "{}"'.format(n)
    results = solr.search(query)
    print(results.hits)

def readTest(n):
    data = []
    for x in range(n):
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: readExecute(x), number=1)
        finally:
            mutex.release()
        print(x)
        data.append([x,time])

    header = ['Number', 'Time']
    with open('dataSelectSolr.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertBurst(n):
    data = []
    file = open("./image/imageset/metadata/descendingOrder.txt", "r")

    for num in range(1):
        images=[]
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
                    time = timeit.timeit(lambda: insertMultiple(images, n), number=1)
                finally:
                    mutex.release()
                if(num == 0):
                    data.append([x, time])
                print('Number: %s', x)
        deleteTable()
        file.seek(0,0)

    header = ['Number', 'Time']

    with open('dataBurstDescendingSolr.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def deleteExecute(n):
    solr.delete(q='metadata:%s' % n)

def deleteTest(n):
    data = []
    for x in range(n):
        mutex.acquire()
        try:
            time = timeit.timeit(lambda: deleteExecute(x), number=1)
        finally:
            mutex.release()
        data.append([x,time])

    header = ['Number', 'Time']
    with open('dataDescendingSolrDelete.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")

def insertIndividual(n):
    data = []
    file = open("./image/imageset/metadata/descendingOrder.txt", "r")

    for num in range(1):
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
                mutex.acquire()
                try:
                    time = timeit.timeit(lambda: insertExecute(image, x), number=1)
                finally:
                    mutex.release()
                
                data.append([x, time])
                print('Number: %s', x)
        deleteTest(n)
        return
        deleteTable()
        file.seek(0,0)

    header = ['Number', 'Time']

    with open('dataDescendingSolr.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        print("Writing line")


solr = pysolr.Solr('http://localhost:8983/solr/solr_sample', always_commit=True)
solr.ping()
deleteTable()

t = Thread(target = insertIndividual, args=[3300])
t.start()
