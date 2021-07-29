# Import Statements
import parsl
from parsl import python_app
from parsl.monitoring import MonitoringHub

import multiprocessing

import os

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher

from parsl.config import Config
from parsl.executors import HighThroughputExecutor


from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.file_noop import NoOpFileStaging

import time

# Initialisation of functions and configurations

cpw = []
#Scanner for Cores of System

cores = multiprocessing.cpu_count()

multiprocessing.cpu_count()

for j in range(1, cores+1):
    cpw.append(cores / j)

working_dir = os.getcwd() + "/" + "test_htex_alternate"

def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                working_dir=working_dir,
                storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                worker_debug=True,
                cores_per_worker = 0.25, # Varies based on list cpw
                heartbeat_period=2,
                heartbeat_threshold=5,
                poll_period=100,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=0,
                    min_blocks=0,
                    max_blocks=5,
                    launcher=SingleNodeLauncher(),
                ),
            )
        ],
        strategy='simple',
        app_cache=True, checkpoint_mode='task_exit',
        retries=2,
        monitoring=MonitoringHub(
                        hub_address="localhost",
                        hub_port=55055,
                        monitoring_debug=False,
                        resource_monitoring_interval=1,
        )
    )

config = fresh_config()

parsl.load(config)

# Applications

@python_app
def app_A():
    a = 2 * 3 + 1
    return a
    time.sleep(2)

@python_app
def app_B():
    b = 2 + 2 / 2
    return b
    time.sleep(4)

@python_app
def app_C(x, y):
    return x + y
    time.sleep(3)

@python_app
def app_D(x, y, z):
    return x * y // z
    time.sleep(1)

@python_app
def app_E(x):
    return x * x
    time.sleep(5)

total = 0

totalCost = []

# Printing statistics for each runtime based on cores per worker

tStart = time.perf_counter()
for i in range(len(cpw)):
    cores_per_worker = cpw[i]
    total = total + app_E(app_D(10, 7, app_C(app_A(), app_B()))).result()
    tEnd = time.perf_counter()
    totalCost.append((tEnd-tStart)*(cores/cores_per_worker))
    print (totalCost)
    tStart = tEnd
    print()
    print("Cores per Worker: " + str(cores_per_worker))
    print("Total: " + str(total))
    print()

# Finding and printing most efficient use of cores per worker

print()
minCost = min(totalCost)
minIndex = totalCost.index(minCost)
optimalCPW = cpw[minIndex]
nodesNecessary = cores / optimalCPW

print(nodesNecessary)

print("")



'''
Goal: Least amount of nodes necessary
Translation: Least amount of workers
workers = C/ (C/W)
Decrement forloop from top to bottom, then we can actually return cores of system / C/W
C/W we have to make it fluid (the list)
'''