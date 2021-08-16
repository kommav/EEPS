# Code used to understand Parsl and how runs and dependacies work
# Does not need to be used in final product

import parsl

# imports for monitoring:
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

working_dir = os.getcwd() + "/" + "test_htex_alternate"


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                address="localhost",
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
                        monitoring_debug=True,
                        resource_monitoring_interval=1,
        )
    )


config = fresh_config()

parsl.load(config)

@parsl.python_app
def sleeper():
  import time
  time.sleep(5)

futures = []
for n in range(0,50):
  futures.append(sleeper())

from concurrent.futures import as_completed
print("waiting")
n = 0
for f in as_completed(futures):
  f.result()
  print(f"got result {n}")
  n += 1

