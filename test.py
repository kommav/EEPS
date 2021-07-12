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


from random import randint
from random import seed

working_dir = os.getcwd() + "/" + "test_htex_alternate"

def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                working_dir=working_dir,
                storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                worker_debug=True,
                cores_per_worker=1,
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
    a = 2 * 3 + 1
    return a

@python_app
def app_B():
    b = 2 + 2 / 2
    return b

@python_app
def app_C(x, y):
    return x + y

def app_D(x, y, z):
    return x * y // 7

def app_E(x):
    return x * x

def app_F():
    iterations = randint(0,10)
    return iterations
total = 0
for x in range(app_F()):
    total += app_E(app_D(10, 7, app_C(app_A(), app_B()).result()))
print(total)
# total will be random but should be iterations * 7
