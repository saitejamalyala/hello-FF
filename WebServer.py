import socket
import sys
import json
import csv
import time
import logging
import os
from datetime import datetime, timedelta
global dataDict, Timeout, dataList

Timeout = datetime.now()
index = 0
path = os.getcwd()
logging.basicConfig(filename='app.log',filemode='a', format='%(asctime)s - %(message)s', level=logging.ERROR)
with open("Config.json", 'r') as inputFile:
    folderDict = json.load(inputFile)

def createFolderStructure():

    for key in folderDict.keys():
        newPath = os.path.join(path, folderDict[key])
        # newPath += ".csv"
        # print(newPath)
        if not os.path.exists(newPath):
            os.mkdir(newPath)


def receiveData(client, timeout = 2):
    Data = client.recv(1024).decode('utf-8')
    begin = time.time()
    if Data:
        while time.time()-begin < timeout and len(Data) <200 and Data[len(Data)-1] != '}':
            Data += client.recv(1024).decode('utf-8')

    return Data

def createCSV(fileName):

    try:
        CSVReader = csv.reader(open(str(fileName) + ".csv", 'r'), delimiter=",", quotechar='|',
                               quoting=csv.QUOTE_MINIMAL)
    except:
        file = open(fileName + ".csv", 'w', newline='')
        CSVWriter = csv.writer(file, delimiter=",", quotechar='|', quoting=csv.QUOTE_MINIMAL)
        CSVWriter.writerow(['Date', 'Time', 'Mean Radiant Temperature (C)', 'Carbon Dioxide (ppm)',
                            'Temperature (C)', 'Humidity (%)'])
        file.close()


def jsonParse(finalData):
    global dataDict, Timeout, index
    try:
        jsonData = json.loads(finalData)

    except:
        logging.error("Error: Invalid Json string")
        print ("invalid Json String")
        return -1
    rowData = [datetime.now().strftime('%Y-%m-%d'), datetime.now().strftime('%H:%M:%S'),
               jsonData["Mean Radiant Temperature (C)"], jsonData["CO2 (ppm)"],
               jsonData["Temperature (C)"],jsonData["Humidity (%)"]]
    for key in folderDict.keys():
        if key == str(jsonData["ID"]):
            fileName = os.path.join(path, folderDict[key], datetime.now().strftime("%B_%Y"))
            # fileName += ".csv"
    createCSV(fileName)
    file = open(fileName + ".csv", 'a', newline='')
    CSVWriter = csv.writer(file, delimiter=",", quotechar='|', quoting=csv.QUOTE_MINIMAL)
    CSVWriter.writerow(rowData)

    # dataDict.update({index:rowData})
    #
    # if Timeout + timedelta(minutes = 5) < datetime.now():
    #
    #     try:
    #         file = open(fileName + '.csv', 'ab')
    #         CSVWriter = csv.writer(file, delimiter=",", quotechar='|', quoting=csv.QUOTE_MINIMAL)
    #         if dataDict:
    #             print (dataDict)
    #             for item in dataDict:
    #                 CSVWriter.writerow(dataDict[item])
    #
    #             dataDict.clear()
    #             index = 0
    #             Timeout = datetime.now()
    #
    #         file.close()

        # except:
        #     print ("File is not free storing data in a dictionary")
        #     logging.warning("File is not free storing data in a dictionary")


    return 0

def main():
    global dataDict
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('192.168.0.102', 10000)
    # print(sys.stderr, 'starting up on %s port %s' % server_address)
    logging.info('starting up on %s port %s' % server_address)

    sock.bind(server_address)
    sock.listen(5)
    while (True):
        print ( sys.stderr, 'waiting for a connection')
        logging.info('waiting for a connection')
        connection, client_address = sock.accept()
        # fileName = datetime.now().strftime("%B_%Y") + "_" + client_address[0]
        # createCSV(fileName)


        try:
            print (sys.stderr, 'connection from', client_address)
            logging.info('connection from', client_address)

            while True:
                data = receiveData(connection)
                # data = receiveData(sock)
                # data = connection.recv(1024)

                if data:
                    # print(len(data))
                    print(sys.stderr, 'received "%s"' %data)
                    logging.info('received "%s"' %data)
                    print(sys.stderr, 'Sending all "%s"' %data)
                    logging.info('Sending all "%s"' %data)
                    connection.sendall(data.encode('utf-8'))
                    jsonParse(data)

                else:
                    print(sys.stderr, "no more data from client")
                    logging.info("no more data from client")
                    break


        # except data:
        #     print(data)

        finally:
            # Clean up the connection
            connection.close()

    socket.close()


if __name__ == "__main__":
    createFolderStructure()
    main()
