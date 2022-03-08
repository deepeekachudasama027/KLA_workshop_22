import yaml
import sys
from datetime import datetime
import time

def function_1(activities, key):
    if activities['Execution'] == 'Sequential':
        function_2(activities['Activities'], key)


def function_2(activities, key):
    for k in activities:
        if activities[k]['Type'] == 'Task':
            print(datetime.now(), end=';')
            print(f'{key}.{k} Entry')
            if activities[k]['Function'] == 'TimeFunction':
                input = activities[k]['Inputs']['FunctionInput']
                process_time = activities[k]['Inputs']['ExecutionTime']
                print(datetime.now(), end=';')
                print(f'{k} Executing TimeFunction {input, process_time}')
                time.sleep(int(process_time))

            print(datetime.now(), end=';')
            print(f'{key}.{k} Exit')

        elif activities[k]['Type'] == 'Flow':
            print(datetime.now(), end=';')
            print(f'{key}.{k} Entry')
            function_1(activities[k], key+k)
            print(datetime.now(), end=';')
            print(f'{key}.{k} Exit')



with open('Milestone1A.yaml', 'r') as file:
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
