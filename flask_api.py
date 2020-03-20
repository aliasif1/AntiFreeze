import json
from flask import Flask,jsonify
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)

@app.route('/')
@cross_origin(origin='*')
def home():
    return jsonify('Hello World')

@app.route('/config', methods = ['GET'])
@cross_origin(origin='*')
def getConfigData():
    try:
        fileData = open('/home/ubuntu/Research/Antifreeze/WorkerSpecs.json','r')
        data = json.load(fileData)
        print(data)
        return jsonify(data)
    except:
        return  jsonify('No Data Found')

@app.route('/stats', methods = ['GET'])
@cross_origin(origin='*')
def getstatsData():
    file_list = ['m-2_con1','m-2_con2','m-3_con1','m-3_con2'] 
    CPU_dict = {}
    CPU_list = []
    individual_container_list = []
    for item in file_list:
        try:
            fileData = open('/home/ubuntu/Research/Antifreeze/{}'.format(item),'r')
            for line in fileData:
                line = line.strip()
                line = line.split()
                if(line[0] == 'CONTAINER'):
                    continue
                cpu_usage = line[2]
                individual_container_list.append(cpu_usage)
        except:
            cpu_usage = 0
            individual_container_list.append(cpu_usage)
        
        CPU_dict[item] = individual_container_list.copy()
        CPU_list.append(CPU_dict.copy())
        CPU_dict.clear()
        individual_container_list.clear()
    return jsonify(CPU_list)

@app.route('/transcode', methods = ['GET'])
@cross_origin(origin='*')
def getTranscodedSegmentData():
    data_list = []
    data_dict = {}
    try:
        fileData = open('/home/ubuntu/Research/Antifreeze/duration','r')
        for line in fileData:
            line = line.strip()
            segment_name,worker_name,container_name,speed = line.split(',')
            data_dict['segment'] = segment_name.strip().split(':')[1]
            data_dict['worker'] = worker_name.strip().split(':')[1]
            data_dict['container'] = container_name.strip().split(':')[1]
            data_dict['speed'] = speed.strip().split(':')[1]
            data_list.append(data_dict.copy())
            data_dict.clear()
    except:
        data_list.append(0)
        
    return jsonify(data_list)

app.run(port=8880,debug=True)
