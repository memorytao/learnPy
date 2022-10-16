
import os

def read_data():
    with open(os.getcwd()+"/confirmed-cases.csv", mode='r') as f:
        return f.readlines()

def transform_data(keys,data):
    cases = []
    case = {}
    for item in data:
        record =  item.strip().split(',')

        for i in range(len(keys)):
            case[keys[i]] = record[i]

        print(case)
        cases.append(case)
        case = {}

    return cases

data = read_data()
keys = data.pop(0).strip().split(',')
cases = transform_data(keys,data)


