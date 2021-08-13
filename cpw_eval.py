# Import Statements..
import parsl
from parsl import python_app
from parsl.monitoring import MonitoringHub
import matplotlib.pyplot as plt
import numpy as np

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

def fresh_config(cpw_input):
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                working_dir=working_dir,
                storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                worker_debug=True,
                cores_per_worker = cpw_input,# Varies based on list cpw
                heartbeat_period=2,
                heartbeat_threshold=5,
                poll_period=100,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=0,
                    min_blocks=0,
                    max_blocks=1,
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

# Applications


@python_app
def app_A():
    time.sleep(2)
    a = 2 * 3 + 1
    return a

@python_app
def app_B(x):
    time.sleep(4)
    b = x + 2 / 2
    return b

@python_app
def app_C(x, y):
    time.sleep(3)
    return x + y

@python_app
def app_D(a,b,c):
    time.sleep(1)
    d = a + b - c
    a = d + b
    return (a / d) + 2

@python_app
def app_E(a,b,c,d):
    time.sleep(7)
    e = a + b - c / d
    a = e * d + b
    return (a / e) + 2

@python_app
def app_F(a,b,c,d,e):
    time.sleep(3)
    return (c - b) + e * (d / a)

@python_app
def app_G():
    time.sleep(9)
    return 9 * 2

@python_app
def app_H(x):
    time.sleep(3)
    return x / 2

@python_app
def app_I(x,y):
    time.sleep(1)
    z = x * x - (3 - 4 * y)
    return (0 - z) + 2

@python_app
def app_J(x,y,z):
    time.sleep(2)
    return (x + y) * z / 2

@python_app
def app_K(a,b,c,d):
    time.sleep(3)
    e = a + b - c / d
    a = e * d + b
    return (a / e) + 2

@python_app
def app_L(a,b,c,d,e):
    time.sleep(1)
    return (c - b) + e * (d / a)

@python_app
def app_M():
    time.sleep(4)
    return 10 * 3 / 5

@python_app
def app_N(x):
    time.sleep(3)
    return x / 2

@python_app
def app_O(x,y):
    time.sleep(6)
    return x / 2 + y

@python_app
def app_P(x,y,z):
    time.sleep(2)
    return y - x + z * y / x

@python_app
def app_Q(a,b,c,d):
    time.sleep(8)
    p = a * a + b / (c - d)
    return p * p / p + p

@python_app
def app_R(a,b,c,d,e):
    time.sleep(2)
    a = a + 9
    b = a + b
    c = b + c
    d = c + d
    e = d + e
    r = e * a
    return r

@python_app
def app_S(a,b,c,d,e,f):
    time.sleep(4)
    return a - f + b * c / d + e

@python_app
def app_T(a,b,c,d,e,f,g):
    time.sleep(3)
    return a + b + c + d + e + f + g

@python_app
def app_U(a,b,c,d,e,f,g,h):
    time.sleep(6)
    return a + b - c * d / e + f - g * h

@python_app
def app_V(a,b,c,d,e,f,g,h,i):
    time.sleep(8)
    a = a * b + 1
    b = b * c + a
    c = c * d + b
    d = d * e + c
    e = e * f + d
    f = f / g + e
    g = g / h + f
    v = g + (h/f)
    return v * 8 - i

@python_app
def app_W(a,b,c,d,e,f,g,h,i,j):
    time.sleep(2)
    w = j - a + e / (c * h)
    return w / 3 + b * d - 2 + f / g * i

@python_app
def app_X(a,b,c,d,e,f,g,h,i,j,k):
    time.sleep(3)
    x = 6 * f - h / g + f
    return x * 3 + (i * j - 4 * k / (a - b + c * d / e))

@python_app
def app_Y(a,b,c,d,e,f,g,h,i,j,k,l):
    time.sleep(1)
    a = a * (c + 8 / d * l)
    b = e - a + 5 * k * (g - 7 + i * b + j / h)
    y = b * 9 / f + 1
    return 18 * y / 2

@python_app
def app_Z(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t):
    time.sleep(6)
    z = (a + b) - c * d + e - f + (g - h) + i / j - (k + l)
    return z * (n + r) / m + o + p - (q * t / s)

total = 0

totalTimes = []
totalCost = []


# Printing statistics for each runtime based on cores per worker

for i in range(len(cpw)):
    cores_per_worker = cpw[i]
    parsl.load(fresh_config(cores_per_worker))
    tStart = time.perf_counter()
    total = app_Z(app_D(app_A(), app_G(), app_M()),
                  app_E(5, 10, 15, 20),
                  app_F(9, app_M(), app_E(19, app_N(6), 24, 34), 45, app_B(8)),
                  app_J(app_H(app_G()), app_D(5, 2, 9), 5),
                  app_K(52, app_A(), 13, 54),
                  app_L(app_N(13), 22, app_H(11), 27, 18),
                  app_P(50, 16, app_M()),
                  app_Q(14, 23, 20, 45),
                  app_R(48, 20, 30, app_O(21, 38), 23),
                  app_S(app_I(47, 7), 29, 48, 3, 5, 24),
                  app_T(4, 11, 46, 36, 48, 38, 6),
                  app_U(25, 29, 36, 12, 7, 14, 10, 50),
                  app_V(44, 30, 35, 10, app_Q(34, 8, 12, 49), 7, 15, 21, 47),
                  app_W(49, 31, app_I(9, 7), 20, 32, 29, 23, 15, 27, 1),
                  app_X(41, 20, app_B(44), 21, 48, 45, 41, 20, app_C(24, 33), 7, 36),
                  app_Y(0, 31, 5, app_N(40), 46, 40, 22, 1, 16, 32, 12, 42),
                  app_A(),
                  app_B(28),
                  app_C(45, app_B(app_P(42, 37, app_M()))),
                  app_M()).result()
    tEnd = time.perf_counter()
    totalTimes.append(tEnd - tStart)
    totalCost.append((tEnd-tStart)*(cores/cores_per_worker))
    print("Total Times")
    print(totalTimes)
    print("Total Costs")
    print(totalCost)
    print()
    print("Cores per worker: " + str(cores_per_worker))
    print("Total: " + str(total))
    print()
    parsl.dfk().cleanup()
    parsl.clear()


nodes = []
for x in range(len(cpw)):
    nodes.append(cores / cpw[x])

w = 0.4
bar1 = np.arange(len(nodes))
bar2 = [i+w for i in bar1]

plt.bar(bar1, totalCost, w, label="Cost")
plt.bar(bar2, totalTimes, w, label="Times")
plt.title("Cost and Time v. Nodes(Cores)")
plt.xticks(bar1 + w/2, nodes)
plt.xlabel('Nodes(Cores)')
plt.ylabel('Time(Seconds) or Cost(Core Seconds)')
plt.legend(loc="upper left")
plt.savefig('CTvN.png')


plt.figure()
plt.plot(totalCost, totalTimes)
plt.title("Time v. Cost")
plt.xlabel('Cost(core seconds)')
plt.ylabel('Time(seconds)')
plt.savefig('CpTvN.png')

# Finding and printing most efficient use of cores per worker

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


# Output Statements

print()
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



'''
Goal: Least amount of nodes necessary
Translation: Least amount of workers
Workers = C/(C/W)
Decrement for loop from top to bottom, then we can actually return cores of system / C/W
C/W we have to make it fluid (the list)
'''