#Update
import parsl
from parsl import python_app
from parsl.monitoring import MonitoringHub


import os
import random

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher

from parsl.config import Config
from parsl.executors import HighThroughputExecutor


from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.file_noop import NoOpFileStaging

import time

t0 = time.perf_counter()

working_dir = os.getcwd() + "/" + "test_htex_alternate"

def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                working_dir=working_dir,
                storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                worker_debug=True,
                cores_per_worker = 0.25,
                max_workers = 3,
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

@python_app
def app_A():
    print("A Started")
    time.sleep(2)
    a = 2 * 3 + 1
    return a
tA = time.perf_counter()

@python_app
def app_B():
    print("B Started")
    time.sleep(3)
    b = 2 + 2 / 2
    return b
tB = time.perf_counter()

@python_app
def app_C(x, y):
    print("C Started")
    time.sleep(5)
    return x + y
tC = time.perf_counter()

@python_app
def app_D(x, y, z):
    print("D Started")
    time.sleep(4)
    return x * y // z
tD = time.perf_counter()

@python_app
def app_E(x):
    print("E Started")
    time.sleep(10)
    return x * x
tE = time.perf_counter()

@python_app
def app_F():
    print("F Started")
    time.sleep(6)
    import random
    from random import randint
    iterations = randint(0,10)
#   return iterations
    return 1 #Test to reduce variability
tF = time.perf_counter()

total = 0
loop = app_F().result()
for x in range(loop):
    total = total + app_E(app_D(10, 7, app_C(app_A(), app_B()))).result()
print()
print("Total: " + str(total))
print("Total Runtime: " + str(tF-t0) + " seconds")
print("Time to run A: " + str(tA-t0) + " seconds")
print("Time to run B: " + str(tB-tA) + " seconds")
print("Time to run C: " + str(tC-tB) + " seconds")
print("Time to run D: " + str(tD-tC) + " seconds")
print("Time to run E: " + str(tE-tD) + " seconds")
print("Time to run F: " + str(tF-tE) + " seconds")

# total will be random but should be iterations * 100
