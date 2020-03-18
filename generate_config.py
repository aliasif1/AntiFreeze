import subprocess
import json
from multiprocessing import Process,pool,current_process,active_children

def GetWorkerSpecifications(worker_list):
    core_config = {} #Dictionery - key = worker name, value = cores on the worker
    core_config_list = []
    for worker in worker_list:
        print('Checking Number of cores on worker : {}'.format(worker))
        coresDetected = subprocess.check_output("ssh {} 'nproc --all'".format(worker),shell=True)
        coresDetected = coresDetected.decode("utf-8").strip() #Change to string and strip the escape characters
        coresDetected = int(coresDetected) #Change to integer
        print('{} cores detected on {}'.format(coresDetected, worker))
        core_config[worker] = coresDetected
        core_config_list.append(core_config.copy())
        core_config.clear()

    return core_config_list

def GetTranscodingCores():
    #Logic to compute the cores required for transcoding
    return 1 #Guess

def getContainersOnWorkers(worker_conf,transcoding_cores):
    worker_container_list = []
    worker_configuration ={}
    uniqueID = 1
    for worker in worker_conf:
        for worker_name in worker:
            containersSupported = int(worker[worker_name]/transcoding_cores)
            worker_configuration['total_cores'] = worker[worker_name]
            worker_configuration['name'] = worker_name
            worker_configuration['uniqueID'] =  uniqueID
            worker_configuration['containers'] = containersSupported
            worker_container_list.append(worker_configuration.copy())
            worker_configuration.clear() #Not required but to be on safe side
            #print(worker_container_list)
            #print(worker_configuration)
            #print('****')
            uniqueID = uniqueID + 1
    return(worker_container_list)

def startContainer(worker_name,container_number,transcoding_cores):
    container_name = 'con'+ str(container_number)
    subprocess.call('ssh {} "docker container run -d --name {} --cpus {} aliasifm/ffmpeg_1 sleep 36000"'.format(worker_name,container_name,transcoding_cores),shell=True)
    print('Successfully started {} on {}'.format(container_name,worker_name))

def generateConfigFile(worker_conf,transcoding_cores,target_resolution):
    config_list =[]
    config = {}
    
    #Set up first element of the list as a general configuration dictionery
    config['coresPerJob'] = transcoding_cores
    config['targetResolution'] = target_resolution
    config_list.append(config.copy())
    config.clear()
    for worker in worker_conf:
        config['nodeId'] = worker['uniqueID']
        config['nodeName'] = worker['name']
        config['containersSupported'] = worker['containers']
        config['totalCores'] = worker['total_cores']
        config['coresPerJob'] = transcoding_cores
        config['freeContainers'] = worker['containers']
        containers = worker['containers']
        i = 1
        config['containerList'] = []
        for container in range(containers):
            name = 'con'+str(i)
            i+=1
            config[name] = 0
            config['containerList'].append(name)
        config_list.append(config.copy())
        config.clear()
    return (config_list)


def WriteWorkerConfigFile(config_data):
    with open('WorkerSpecs.json','w') as ff:
        json.dump(config_data,ff)


target_resolution = '720x480' #Have to be changed
worker_list = ['m-2','m-3'] #read from a config file
worker_conf = GetWorkerSpecifications(worker_list)
print(worker_conf)
transcoding_cores = GetTranscodingCores() #Function of Resolution but may be initialized
worker_container_list = getContainersOnWorkers(worker_conf,transcoding_cores)
print(worker_container_list)


process_list = []
process_index = 1
#Setup the Containers launching process
for worker in worker_container_list:
    name = worker['name']
    containers  =  int(worker['containers'])
    for num in range(containers):
        processNum = 'process'+ str(process_index)
        processNum = Process(target=startContainer,args=(name,num+1,transcoding_cores))
        process_list.append(processNum)
        process_index = process_index + 1

#Start the process
for p in process_list:
    p.start()

#Wait for the containers to be launched
for p in process_list:
    p.join()

#Generate the config data 
config_list = generateConfigFile(worker_container_list,transcoding_cores,target_resolution)
print(config_list)

#Save the config data to a config file
WriteWorkerConfigFile(config_list)
