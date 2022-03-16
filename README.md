PyMultiTasking
===============


----
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://choosealicense.com/licenses/gpl-3.0/)
[![Docs](https://readthedocs.org/projects/ansicolortags/badge/?version=latest)](https://orcephrye.github.io/PyMultiTasking/)

A set of tools for achieving concurrency in Python 3 easily. 

### PyMultiTasking Paradigm

---

This follows the paradigm of **Worker/Pool/Task/Decorator** for both Threading and Multi-Process (future update). 

* **Worker**: A ThreadWorker inherits from  threading.Thread. It has additional parameters that extend is functionality. 
Examples includes adding extra support too easily kill or otherwise stop the thread, a queue so it can keep receiving 
more functions to execute and so on.

* **Pool**: Manages multiple Workers. Pools use a PriorityQueue to send Tasks into a given number of Threads/Processes. 
It can attempt to spin up and down the number of necessary Workers depending on the number of tasks and the number of 
CPU Cores on the machine. A programmer can adjust the number of workers in a pool as well. The Pool provides many 
additional methods, properties, functionality that is helpful when dealing with large amounts of threads.

* **Task**: A Task object inherits from threading.Event and thus can be waited on. The event is set when the Task is 
complete. The Task can also sync with other events via a Semaphore. It saves the results of a given function, it can
handle a call back function, it can inject itself into given functions whenever possible, and it is serializable, and
much more.

* **Decorator**: This toolkit comes with 3 Decorators for threading. 'Limiter', 'Threaded', and 'Processed' 
(future update). Limiter works to limit how many times a function/method can be run simultaneously. While Threaded is a
simple way to make a Function/Method threaded. Whenever @Threaded is used the wrapped function returns a Task instead
of its usual results. The results are saved in <TaskObject>.results. Threaded has many arguments that can adjust its 
behavior which are explored below.

The above explanation is how both Threaded and the soon to be released Process tools will follow. Regardless of using
threading or multiprocess the Workers/Pools/Decorators will use the same Task object type. While Pools are called 
either ThreadPools/ProcessPools and the same goes for workers.  

### PoolExamples:

---

Below are examples using the ThreadingPool class. Future releases will include the ProcesingPool which will follow the 
same paradigm and have the same features.

```python
# A simple function to be used in further examples:
def test(*args, **kwargs):
    print(f'{args} - {kwargs}') 
```

To work directly with a Pool and submit a tasks:
```python
from PyThreadingPool.ThreadingPool import Pool
p1 = Pool()
task = p1.submit(test, "arg", example="kwargs")
```

Submit returns a Task object. Task stores results and is also an Event object. Below example also shows the
'submit_task_priority' priority keyword can provide the priority that the task should be treated when it is put into
the TaskQueue. 
```python
from PyMultiTasking import ThreadPool as Pool
p1 = Pool()
task = p1.submit(test, "arg", example="kwargs", submit_task_priority=100) # The default priority is 10
if task.wait(10):
    print(task.results)
else:
    print(f"{task} not finished")
```

You can submit partials as well. This can be helpful if you want to wrap a function with certain arguments and then 
later submit that function with additional arguments.
```python
from functools import partial
from PyMultiTasking import ThreadPool as Pool
func = partial(test, "arg", example="kwargs")
p1 = Pool()
p1.submit(func)
```

You can use the 'map' method to execute the same method multiple times with a list of arguments. You will get a list of
tasks back. Map also has an optional chunksize parameter by default it is 0 which then will attempt to spread given 
tasks out evenly across the workers.
```python
from PyMultiTasking import ThreadPool as Pool
arg1 = (("arg",), {"example": "kwargs"})
arg2 = (("arg2",), {"example2": "kwargs"})
p1 = Pool()
tasks = p1.map(test, [arg1, arg2])
```

A pool can wait on all tasks to be complete. The 'block' keyword can be used to block new tasks from being added.
```python
from PyMultiTasking import ThreadPool as Pool
arg1 = (("arg",), {"example": "kwargs"})
arg2 = (("arg2",), {"example2": "kwargs"})
p1 = Pool()
p1.map(test, [arg1, arg2])
if p1.wait_completion(10, block=True):
    print("The pool is finished work its current work load")
```

The Pool method 'as_completed' is a generator that returns tasks as they complete.
```python
from PyMultiTasking import ThreadPool as Pool
arg1 = (("arg",), {"example": "kwargs"})
arg2 = (("arg2",), {"example2": "kwargs"})

p1 = Pool()
tasks = p1.map(test, [arg1, arg2])

# 'as_completed' is a staticmethod of ThreadPool
for task in Pool.as_completed(tasks):
    print(f"Task[{task}] completed with results: {task.results}")
```

### Decorator Examples

---
Currently, there is only the 'Threaded' decorator for multitasking. However, future releases will have the Processed 
decorator and will have all the same features.

You can make any function or method threaded by simply using the decorator. Each time the 'test' method is called 
it will now respond with a Task object and a Worker will be spawned to execute the function code block.
```python
from PyMultiTasking import Threaded

@Threaded
def test(*args, **kwargs):
    print(f'{args} - {kwargs}')

task = test()
if task.wait(10):
    print(task.results)
else:
    print(f"{task} not finished")
```

Threaded also has the 'daemon' parameter. This spawns a single Worker thread and keeps it alive instead of ending it 
once the function is done. Now every time the 'test' method is called it simply pushes the function call to the Worker.
The worker is cleaned when Python exists. It also can respawn a Worker if the previous worker crashed due to an 
uncaught exception.
```python
from PyMultiTasking import Threaded

@Threaded(daemon=True)
def test(*args, **kwargs):
    print(f'{args} - {kwargs}')
```

Threaded can also spawn a Pool for the function. This can be a useful alternative to 'daemon' to have more than 1 
Worker assigned to the method. It is best practices to shutdown the pool before exiting. All pools are registered which
makes this easy.
```python
from PyMultiTasking import Threaded, ThreadPool as Pool

@Threaded(pool=True)
def test(*args, **kwargs):
    print(f'{args} - {kwargs}')

Pool.join_pools(timeout=60) # This will find all Pools and run 'join' on them.
```

Threaded can also use the 'pool_name' keyword. This by default makes 'pool' keyword be True and will spawn a new Pool
with the name provided if one doesn't already exist in the Pool registry. This is useful if the developer wants to 
use a Pool across multiple functions and/or want to join/block/wait on the Pool and thus all associated functions.
```python
from PyMultiTasking import Threaded, ThreadPool as Pool
pool_name = 'mypool' # Global variable


@Threaded(pool_name=pool_name)
def test_dec_pool_by_name_one(*args, **kwargs):
    print(f"Running: test_dec_pool_by_name_one - args={args} - kwargs={kwargs}")


@Threaded(pool_name=pool_name)
def test_dec_pool_by_name_twp(*args, **kwargs):
    print(f"Running: test_dec_pool_by_name_twp - args={args} - kwargs={kwargs}")

    
tasksOne = [test_dec_pool_by_name_one(randomsleep=True) for _ in range(4)]
tasksTwo = [test_dec_pool_by_name_twp(randomsleep=True) for _ in range(4)]

pool = Pool.get_pool_by_name(pool_name) # Get the Pool associated with the two above functions
pool.wait_completion(block=True) # Block all new submissions until existing work has been completed 

for task in tasksOne + tasksTwo:
    print(f"Task[{task}] completed with results: {task.results}")
```

Threaded can also pass a call back function to the Task it creates when wrapping the method with the 'callback_func'
parameter. Learn more about the callback_func parameter by reading the Task class documentation.
```python
from PyMultiTasking import Threaded

def callback_test(*args, **kwargs):
    print(f'callback_test - Completed: {args} - {kwargs}')
    return args[0]

@Threaded(callback_func=callback_test)
def test(*args, **kwargs):
    print(f'{args} - {kwargs}')
```

Threaded can also pass parameters directly to the Task/Worker/Pool. It can submit information to a Task with 
'_task_<keyword>' and to a worker following the same schema '_worker_<keyword>' and again the same for pool 
ie: '_pool_<keyword>'

Let's combine some of the above things together now. 
```python
from random import randint
from PyMultiTasking import Limiter, Threaded, ThreadPool as Pool
store_values = [] # Global variable used because the task isn't going to store the results.

# Learn about the Limiter in the following section.
@Limiter(1) # Because of the limiter the store_values object will not be appended to more than one at a time.
def callback_test(*args, **kwargs):
    # This will receive the return value of each 'test' method call.
    global store_values
    print(f'callback_test - Completed: {args} - {kwargs}')
    store_values.append(args)
    

@Threaded(callback_func=callback_test, pool_name='mypool', _pool_maxWorkers=2, _task_store_return=False)
def test(*args, **kwargs):
    print(f'{args} - {kwargs}')
    return randint(0, 10) # Return a random number from 0 to 10.
    

tasks = [test() for _ in range(4)]

pool = Pool.get_pool_by_name('mypool') # Get the Pool associated with the two above functions.
pool.join(block=True) # Ends the Pool.

print(f"Stored Values: {store_values}")
```

### The Limiter 

---
A decorator to control how often functions/methods are handled by threads. 

To simply make a function's execution thread safe. This makes it so the 'test' function can only be executed one at a 
time. Adjust the amount by changing first parameter also named 'num'. 
```python
from PyMultiTasking import Limiter, ThreadPool as Pool

@Limiter(1)
def test(*args, **kwargs):
    print(f'{args} - {kwargs}')

testResults = [test() for _ in range(4)] # This would pause the loop at every 'test' method call
```

Limiter also has a keyword argument 'blocking' which defaults to True. If 'blocking' is True Limiter will acquire the 
lock before executing the wrapped function and let it go after the function is complete. If False the Limiter will pass 
a semephore onto the function as an argument '_task_semaphore'. This assumes that Limiter is wrapping another decorator 
like Threaded but this is not necessary as long as the wrapped function is able to handle the '_task_semaphore' 
parameter. 

The following example will not block the calling of the function even if it is called more than twice. In the example
below it is called 4 times in a loop. All 4 calls would execute quickly and return Tasks and have spawned 4 Workers. 
However, two of the Workers would be waiting for a lock. Once any one of the active Workers completes the Task and 
releases the lock another Worker can then execute their task.
```python
from PyMultiTasking import Limiter, Threaded, ThreadPool as Pool

@Limiter(2, blocking=False)
@Threaded
def test(*args, **kwargs):
    print(f'{args} - {kwargs}')

tasks = [test() for _ in range(4)] # This loop would complete quickly as it doesn't wait for 'test' to actually finish

for task in Pool.as_completed(tasks):
    print(f"Task[{task}] completed with results: {task.results}")
```

* NOTE: Do not use a Limiter if it is wrapping Threaded(daemon=True). Although this technically would not break
anything it is pointless.