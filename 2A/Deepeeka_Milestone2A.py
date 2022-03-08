from email import header
import yaml
import csv
import sys
from datetime import datetime
import time
from threading import Thread
import asyncio

rows = []
file_name = ""
def function_1(activities, key):
    if activities['Execution'] == 'Sequential':
        function_2(activities['Activities'], key)
    elif activities['Execution'] == 'Concurrent':
        function_3(activities['Activities'], key)


def function_2(activities, key):
    for k in activities:
        if activities[k]['Type'] == 'Task':
            print(datetime.now(), end=';')
            print(f'{key}.{k} Entry')
            if activities[k]['Function'] == 'TimeFunction':
                # input = activities[k]['Inputs']['FunctionInput']
                # process_time = activities[k]['Inputs']['ExecutionTime']
                # print(datetime.now(), end=';')
                # print(f'{key}.{k} Executing TimeFunction ({input}, {process_time})')
                # time.sleep(int(process_time))
                loop = asyncio.get_event_loop()
                loop.run_until_complete(csvreader())
                loop.close()
                print(datetime.now(), end=';')
                print(f'{key}.{k} Entry')
                print(f'{key}.{k} Executing Dataload ({file_name})')
                print(datetime.now(), end=';')
                print(f'{key}.{k} Exit')
                
            if activities[k]['Function'] == 'DataLoad':
                function_6(activities, key, k)

            print(datetime.now(), end=';')
            print(f'{key}.{k} Exit')

        elif activities[k]['Type'] == 'Flow':
            print(datetime.now(), end=';')
            print(f'{key}.{k} Entry')
            function_1(activities[k], key+'.'+k)
            print(datetime.now(), end=';')
            print(f'{key}.{k} Exit')

        function_6(activities, key, k)


def function_3(activities, key):
    threads = []
    a = []
    for k in activities:
        if activities[k]['Type'] == 'Task':
            t = Thread(target=function_4, args=(activities, key, k))
            threads.append(t)
            t.start()
            string = "{}.{}".format(key, k)
            a.append(string)
            continue

        elif activities[k]['Type'] == 'Flow':
            t = Thread(target=function_5, args=(activities, key, k))
            threads.append(t)
            t.start()
            string = "{}.{}".format(key, k)
            a.append(string)
            continue

    for t in threads:
        t.join()


def function_4(activities, key, k):
    print(datetime.now(), end=';')
    print(f'{key}.{k} Entry')
    if activities[k]['Function'] == 'TimeFunction':
        input = activities[k]['Inputs']['FunctionInput']
        process_time = activities[k]['Inputs']['ExecutionTime']
        print(datetime.now(), end=';')
        print(f'{key}.{k} Executing TimeFunction ({input}, {process_time})')
        time.sleep(int(process_time))

    print(datetime.now(), end=';')
    print(f'{key}.{k} Exit')


def function_5(activities, key, k):
    print(datetime.now(), end=';')
    print(f'{key}.{k} Entry')
    function_1(activities[k], key+'.'+k)
    print(datetime.now(), end=';')
    print(f'{key}.{k} Exit')


def function_6(activities, key, k):
    header
    if activities[k]['Function'] == 'Dataload':
        if activities[k]['Inputs']['Filename']:
            file_name = activities[k]['Inputs']['Filename']
            csvreader(activities,k,rows,header)
            print(datetime.now(), end=';')
            print(f'{key}.{k} Executing Dataload ({file_name})')
            print(datetime.now(), end=';')
            print(f'{key}.{k} Exit')
        
        if activities[k]['Condition']:
            string=str('$(')
            ckey=string.partition('.NoOfDefects)')
            key = ckey[0]
            cond = ckey[1][0]
            condint =ckey[1][2]
            
            
            

        # elif activities[k]['Inputs']['functionInput']:
        #     loop = asyncio.get_event_loop()
        #     loop.run_until_complete(csvreader())
        #     loop.close()
        #     print(datetime.now(), end=';')
        #     print(f'{key}.{k} Entry')
        #     print(f'{key}.{k} Executing Dataload ({file_name})')
        #     print(datetime.now(), end=';')
        #     print(f'{key}.{k} Exit')

# def csvreader(file_name,header,rows):
#     with open(file_name, 'r') as file:
#         csvreader = csv.reader(file)
#         header = next(csvreader)
#         for row in csvreader:
#             rows.append(row)

async def csvreader(activities,k,rows,header):
    if activities[k]['Inputs']['Filename']:
        file_name = activities[k]['Inputs']['Filename']
        with open(file_name, 'r') as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            for row in csvreader:
                rows.append(row)


with open('Milestone1B.yaml', 'r') as file:
    my_dict = yaml.load(file, Loader=yaml.FullLoader)

stdoutOrigin = sys.stdout
sys.stdout = open("log.txt", "w")

key = list(my_dict.keys())[0]
print(datetime.now(), end=';')
print(f'{key} Entry')
function_1(my_dict[key], key)
print(datetime.now(), end=';')
print(f'{key} Exit')

sys.stdout.close()
sys.stdout = stdoutOrigin
