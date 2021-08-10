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

config = fresh_config()

parsl.load(config)


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
    return x/2

@python_app
def app_U(a,b,c,d,e,f,g,h):
    time.sleep(6)
    return x/2

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
def app_Z(a,b,c,d,e,f,g,h,i,j,k,l,m):
    time.sleep(6)
    z = (a + b) - c * d + e - f + (g - h) + i / j - (k + l)
    return z * 3 / m + 1

total = 0

t0 = time.perf_counter()

total = app_E(app_D(app_G(10), app_F(7), app_C(app_A(), app_B()))).result()

tFinal = time.perf_counter()
print()
print("Total: " + str(total))
print("Total Runtime: " + str(tFinal-t0) + " seconds")
