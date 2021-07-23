
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
                cores_per_worker = 4,
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
    import time
    print("A Started")
    time.sleep(2)
    a = 2 * 3 + 1
#   tA = time.perf_counter()
    return a

@python_app
def app_B():
    import time
    print("B Started")
    time.sleep(3)
    b = 2 + 2 / 2
#   tB = time.perf_counter()
    return b

@python_app
def app_C(x, y):
    import time
    print("C Started")
    time.sleep(5)
#   tC = time.perf_counter()
    return x + y

@python_app
def app_D(x, y, z):
    import time
    print("D Started")
    time.sleep(4)
#   tD = time.perf_counter()
    return x * y // z

@python_app
def app_E(x):
    import time
    print("E Started")
    time.sleep(10)
#  tE = time.perf_counter()
    return x * x

@python_app
def app_F():
    import time
    print("F Started")
    time.sleep(6)
    import random
    from random import randint
    iterations = randint(0,10)
#   return iterations
#    tF = time.perf_counter()
    return 1 #Test to reduce variability

total = 0
loop = app_F().result()
for x in range(loop):
    total = total + app_E(app_D(10, 7, app_C(app_A(), app_B()))).result()
tFinal = time.perf_counter()
print()
print("Total: " + str(total))
print("Total Runtime: " + str(tFinal-t0) + " seconds")
'''
print("Time to run A: " + str(tA-t0) + " seconds")
print("Time to run B: " + str(tB-tA) + " seconds")
print("Time to run C: " + str(tC-tB) + " seconds")
print("Time to run D: " + str(tD-tC) + " seconds")
print("Time to run E: " + str(tE-tD) + " seconds")
print("Time to run F: " + str(tF-tE) + " seconds")
'''
