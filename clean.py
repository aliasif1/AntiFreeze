import subprocess

def CleanWorkers(workers):
    for worker in workers:
        subprocess.call('ssh {} "docker container rm -f con1"'.format(worker),shell = True)
        subprocess.call('ssh {} "docker container rm -f con2"'.format(worker),shell = True)

workers = ['m-2','m-3']
CleanWorkers(workers)
