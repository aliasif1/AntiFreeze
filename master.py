import json
import time
import os
from multiprocessing import Process, Lock, current_process, active_children,Lock
import subprocess
import signal

def checkForNewSegments(input_segment_location):
    while True:
        print('*******Checking for Segments to be transcoded*************')
        segment_list =  sorted(list(os.listdir(input_segment_location)))
        amount = checkForVacantContainers()
        if(len(segment_list) != 0 and amount != 0):           
                if amount >= len(segment_list):
                    print(segment_list)
                    return(segment_list)
                else:
                    print(segment_list)
                    return(segment_list[0:amount])
        time.sleep(0.5)

def checkForVacantContainers():
    with open('WorkerSpecs.json','r') as ff:
        data = json.load(ff)
    freeContainerCount = 0
    for specs in data[1:]:
        freeContainerCount = freeContainerCount + specs['freeContainers']
    print(data)
    print(freeContainerCount)
    return freeContainerCount

def getProcessingData(segments):
    segment_count = len(segments)
    with open('WorkerSpecs.json','r') as ff:
        data = json.load(ff)
    transcoding_cores = data[0]['coresPerJob']
    target_resolution = data[0]['targetResolution']
    container_outer_list = []
    api_index = 0
    for worker in data[1:]:
        api_index+=1
        container_inner_list = []
        workername = worker['nodeName']
        availableContainers = worker['freeContainers']
        container_inner_list.append(workername)
        container_inner_list.append(availableContainers)
        container_inner_list.append(api_index)
        containers = len(worker['containerList'])
        for i in range(1,containers+1):
            if worker['con'+str(i)] == 0:
                container_inner_list.append('con'+str(i))
        container_outer_list.append(container_inner_list)
    print('WwWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW')
    print(container_outer_list)
    print(len(container_outer_list))

    segment_conf_list = []
    segment_conf = []
    for segment in segments:
        maximum = 0
        index = 0
        for i in range(len(container_outer_list)):
            length = len(container_outer_list[i])
            if (length>maximum):
                maximum = length
                index = i
        segment_conf.append(container_outer_list[index][0])
        segment_conf.append(container_outer_list[index][2])
        segment_conf.append(container_outer_list[index][3])
        segment_conf.append(segment)
        container_outer_list[index].pop(3)
        segment_conf_list.append(segment_conf.copy())
        segment_conf.clear()
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    print(segment_conf_list)

    return (segment_conf_list,transcoding_cores,target_resolution)

        

def distributeSegments(sourcePath, destinationPath, segment_conf_list, processedVideoSegments):
    commands = []
    for segmentData in segment_conf_list:
        segment_name = segmentData[3]
        worker = segmentData[0]
        command = "scp {0}/{1} {2}:{3} && mv {0}/{1} {4}".format(sourcePath, segment_name, worker, destinationPath,processedVideoSegments)
        commands.append(command)
    return commands


def distributeSegmentsAndReserveContainers(sourcePath, destinationPath, segment_conf_list, copied_video_segments_path):
    process_array = []
    for segmentData in segment_conf_list:
        p = Process(target=ParallelDistributionAndReservation,args=(segmentData,sourcePath,destinationPath,copied_video_segments_path))
        process_array.append(p)
    
    for p in process_array:
        p.start()
    
    for p in process_array:
        p.join()
    
    return ('Success')
            
def ParallelDistributionAndReservation(segmentData,sourcePath,destinationPath,copied_video_segments_path):
    segment_name = segmentData[3]
    worker = segmentData[0]
    api_index = segmentData[1]
    container_name = segmentData[2]
    #Step 1
    #Copy over the segments
    subprocess.call('scp {0}/{1} {2}:{3} && mv {0}/{1} {4}'.format(sourcePath, segment_name, worker, destinationPath,copied_video_segments_path),shell=True)

    #Step2
    #Change the configFile
    lock.acquire()
    with open('WorkerSpecs.json','r') as ff:
        data = json.load(ff)
    data[api_index][container_name] = 1
    data[api_index]['freeContainers'] = data[api_index]['freeContainers'] - 1
    
    with open('WorkerSpecs.json','w') as ff:
        json.dump(data,ff)
    lock.release()

    
def Transcode(segmentData,transcoding_cores,target_resolution,segment_path,remote_segment_destination,copied_segment_location):
    segment_name = segmentData[3]
    worker = segmentData[0]
    api_index = segmentData[1]
    container_name = segmentData[2]
    #Step 1
    #Copy over the segments
    # subprocess.call('scp {}/{} {}:{} && mv {}/{} {} '.format(segment_path,segment_name,worker,remote_segment_destination,segment_path,segment_name,copied_segment_location),shell=True)
    # print('copied segment {}'.format(segment_name))

    #Step2
    #Change the configFile
    # lock.acquire()
    # with open('WorkerSpecs.json','r') as ff:
    #     data = json.load(ff)
    # data[api_index][container_name] = 1
    # data[api_index]['freeContainers'] = data[api_index]['freeContainers'] - 1
    
    # with open('WorkerSpecs.json','w') as ff:
    #     json.dump(data,ff)
    # lock.release()

    #Step3
    #Set up the stats recording
    #log = open(str(worker)+'_'+str(container_name),'a')
    stats_file_name = str(worker)+'_'+str(container_name)
    statsMetrics = subprocess.Popen('ssh {} "{}/stats.sh {}" >> {}'.format(worker,remote_code_path,container_name, stats_file_name),shell=True,stdout=subprocess.PIPE,preexec_fn=os.setsid)
    
    #Start The transcoding
    start_time = time.time()
    subprocess.call('ssh {} "docker container exec {} ffmpeg -i Input/{} -c:v libx265 -preset ultrafast -tune zerolatency -x265-params frame-threads=8:pools=none:crf=28  -vf scale={} -c:a copy Out/R_{}"'.format(worker,container_name,segment_name,target_resolution,segment_name),shell=True)
    end_time = time.time()
    print('transcoded segment {} ************'.format(segment_name))

    #Stop the metric calculation
    #Stop the CPU Utilization Collection
    os.killpg(os.getpgid(statsMetrics.pid), signal.SIGTERM)

    #log.close()
    
    #Step4
    #Update The Config file Again and save the transcoding duration to duration file
    lock.acquire()
    with open('WorkerSpecs.json','r') as ff:
        data = json.load(ff)
    data[api_index][container_name] = 0
    data[api_index]['freeContainers'] = data[api_index]['freeContainers'] + 1
    
    with open('WorkerSpecs.json','w') as ff:
        json.dump(data,ff)

    with open('duration','a') as ff:
        ff.write('segment:{},worker:{},container:{},speed:{}\n'.format(segment_name,worker,container_name,end_time - start_time))

    lock.release()

    #Step5
    #Get the transcoded segments back to the master
    lock.acquire()
    subprocess.call('scp {}:{}/Out/R_{} {}/ProcessedSegments/'.format(worker,remote_segment_mount,segment_name, project_home_directory),shell=True)
    #subprocess.call('python3 /home/asifm/Research/makeMPD.py',shell=True)
    lock.release()


    

project_home_directory = '/home/ubuntu/Research/Antifreeze'
segment_location = '/home/ubuntu/Research/Antifreeze/Segments'
copied_segment_location = '/home/ubuntu/Research/Antifreeze/CopiedSegments'
remote_segment_path = '/home/ubuntu/Research/Antifreeze/Mount/Input'
remote_segment_mount = '/home/ubuntu/Research/Antifreeze/Mount'
remote_code_path = '/home/ubuntu/Research/Antifreeze/Codes'

# segment_list = checkForNewSegments(segment_location)

# segment_conf_list,transcoding_cores,target_resolution = getProcessingData(segment_list)

# print(segment_conf_list)
# print(transcoding_cores)
# print(target_resolution)

# print('Halt')


lock = Lock()
while (True):
    print('Looking for more Segments')
    active_children() #Kill of sub processes which have finished without blocking the main code
    segment_list = checkForNewSegments(segment_location) #Check For new segments. This function only returns when it finds new segments
    segment_conf_list,transcoding_cores,target_resolution = getProcessingData(segment_list) #get data about which segment goes to which node and which container
    print(segment_conf_list) #list like [nodeName,api index,container name ,segment name]
    print(transcoding_cores)
    print(target_resolution)
    print('halt')

    #Copy over the segments , move the segment and reserve the container
    msg = distributeSegmentsAndReserveContainers(segment_location, remote_segment_path, segment_conf_list, copied_segment_location)
    print(msg)

  

    for segment_data in segment_conf_list:
        p = Process(target=Transcode,args = (segment_data,transcoding_cores,target_resolution,segment_location,remote_segment_path,copied_segment_location))
        p.start()

    #print('Wave one done')
