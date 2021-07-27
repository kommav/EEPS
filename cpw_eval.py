# Import Statements
import parsl
from parsl import python_app
from parsl.monitoring import MonitoringHub


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

t0 = time.perf_counter()

cpw = [0.25, 0.5, 0.75, 1, 2, 3, 4]

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

@python_app
def app_B():
    b = 2 + 2 / 2
    return b

@python_app
def app_C(x, y):
    return x + y

@python_app
def app_D(x, y, z):
    return x * y // z

@python_app
def app_E(x):
    return x * x

@python_app
def app_F():
    return 1

total = 0

totalTimes = []

# Printing statistics for each runtime based on cores per worker

loop = app_F().result()
for i in range(len(cpw)):
    cores_per_worker = cpw[i]
    for x in range(loop):
        total = total + app_E(app_D(10, 7, app_C(app_A(), app_B()))).result()
    tFinal = time.perf_counter()
    totalTimes.append(tFinal)
    print()
    print("Cores per Worker: " + str(cores_per_worker))
    print("Total: " + str(total))
    print("Total Runtime: " + str(tFinal-t0) + " seconds")
    print()

# Finding and printing most efficient use of cores per worker

print()
minTime = min(totalTimes)
minIndex = totalTimes.index(minTime)

if minIndex == 0:
    print("The most efficient cores-per-worker ratio is 0.25")
elif minIndex == 1:
    print("The most efficient cores-per-worker ratio is 0.5")
elif minIndex == 2:
    print("The most efficient cores-per-worker ratio is 0.75")
elif minIndex == 3:
    print("The most efficient cores-per-worker ratio is 1")
elif minIndex == 4:
    print("The most efficient cores-per-worker ratio is 2")
elif minIndex == 5:
    print("The most efficient cores-per-worker ratio is 3")
elif minIndex == 6:
    print("The most efficient cores-per-worker ratio is 4")

print("")