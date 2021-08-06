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


# Scanner for Cores of System

cpw = []

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
    time.sleep(2)
    a = 2 * 3 + 1
    return a

@python_app
def app_B():
    time.sleep(4)
    b = 2 + 2 / 2
    return b


@python_app
def app_C(x, y):
    time.sleep(3)
    return x + y


@python_app
def app_D(x, y, z):
    time.sleep(1)
    return x * y // z


@python_app
def app_E(x):
    time.sleep(5)
    return x * x

@python_app
def app_F(x):
    time.sleep(9)
    return 2*x

@python_app
def app_G(x):
    time.sleep(3)
    return x/2

total = 0

totalTimes = []
totalCost = []


# Printing statistics for each runtime based on cores per worker

tStart = time.perf_counter()
for i in range(len(cpw)):
    cores_per_worker = cpw[i]
    total = total + app_E(app_D(app_G(10), app_F(7), app_C(app_A(), app_B()))).result()
    tEnd = time.perf_counter()
    totalTimes.append(tEnd - tStart)
    totalCost.append((tEnd-tStart)*(cores/cores_per_worker))
    print(totalCost)
    tStart = tEnd
    print()
    print("Cores per worker: " + str(cores_per_worker))
    print("Total: " + str(total))
    print()


# Finding and printing most efficient use of cores per worker

print()
minCost = min(totalCost)
minIndex = totalCost.index(minCost)
optimalCPW = cpw[minIndex]
nodesNecessary = cores / optimalCPW
totalCost.remove(minCost)
secondCost = min(totalCost)
secondIndex = totalCost.index(secondCost) + 1
secondCPW = cpw[secondIndex]
secondNodes = cores / secondCPW
minTime = min(totalTimes)
fastIndex = totalTimes.index(minTime)
fastCPW = cpw[fastIndex]
fastNodes = cores / fastCPW
fastCost = minTime * fastNodes
totalTimes.remove(minTime)
secondTime = min(totalTimes)
pctCost = ((secondCost / minCost) - 1) * 100
pctTime = ((secondTime / minTime) - 1) * 100

round(secondCost, 2)
round(secondCost / secondNodes, 2)


# Searching and Sorting Lists

def sort(arr):
   for i in range(1, len(arr)):
      temp = arr[i]
      pos = search(arr, temp, 0, i) + 1
      for k in range(i, pos, -1):
         arr[k] = arr[k - 1]
      arr[pos] = temp

def search(arr, key, start, end):
   if end - start <= 1:
      if key < arr[start]:
         return start - 1
      else:
         return start
   mid = (start + end)//2
   if arr[mid] < key:
      return search(arr, key, mid, end)
   elif arr[mid] > key:
      return search(arr, key, start, mid)
   else:
      return mid


# Output Statements

print("Cheapest: ")
print("Optimal number of cores: " + str(nodesNecessary))
print("Core seconds: " + str(round(minCost, 2)))
print("Seconds: " + str(round(minCost / nodesNecessary, 2)))
print(" ")
print("Second Cheapest: ")
print("Number of Cores: " + str(secondNodes))
print("Core seconds: " + str(round(secondCost, 2)))
print("Seconds: " + str(round(secondCost / secondNodes, 2)))
print(str(round(secondCost-minCost, 2)) + " core seconds off optimal value")
print("Percentage greater than optimal cost: " + str(round(pctCost, 2)) + "%")
print(" ")
print("Fastest: ")
print("Number of Cores: " + str(fastNodes))
print("Core seconds: " + str(round(fastCost, 2)))
print("Seconds: " + str(round(minTime, 2)))
print(str(round(secondTime - minTime, 2)) + " seconds faster than next fastest")
print("Percentage faster: " + str(round(pctTime, 2)) + "%")
print()


# Testing Sort

print("Initial:" + totalTimes)
sort(totalTimes)
print("Sorted:" + totalTimes)

'''
Goal: Least amount of nodes necessary
Translation: Least amount of workers
Workers = C/(C/W)
Decrement for loop from top to bottom, then we can actually return cores of system / C/W
C/W we have to make it fluid (the list)
'''