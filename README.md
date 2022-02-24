Threading Tools
===============


----
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://choosealicense.com/licenses/gpl-3.0/)
[![Docs](https://readthedocs.org/projects/ansicolortags/badge/?version=latest)](https://orcephrye.github.io/PyMultiTasking/)

A set of tools for making threading in Python 3 easy. 

This follows a theme of a class called Task that is run by a clas called Worker which is managed in a classs called 
Pool. The Pool uses a class called PriorityTaskQueue that inherits from PriorityQueue. 

The average use case is to make a new instance of the Pool class and then submit functions via the 'submit' method. The
'submit' method does not wait on the task to complete it simply submits it to a queue.

#### Examples:

A simple function to be used in further examples:
```python
def test(*args, **kwargs):
    print(f'{args} - {kwargs}') 
```
Simple submit of a task with arguments:
```python
from PyThreadingPool.ThreadingPool import Pool
p1 = Pool()
p1.submit(test, "arg", example="kwargs")
```
You can specify the priority that a new task will be added. This uses the PriorityTaskQueue which will change the order
of that the tasks are worked.
```python
from PyThreadingPool.ThreadingPool import Pool
p1 = Pool()
p1.submit(test, "arg", example="kwargs", submit_task_priority=100)
```
You can submit partials as well:
```python
from functools import partial
from PyThreadingPool.ThreadingPool import Pool
func = partial(test, "arg", example="kwargs")
p1 = Pool()
p1.submit(func)
```
You can use the 'map' method to execute the same method multiple times with a list of arguments
```python
from PyThreadingPool.ThreadingPool import Pool
arg1 = (("arg",), {"example": "kwargs"})
arg2 = (("arg2",), {"example2": "kwargs"})
listOfArguments = [arg1, arg2]
p1 = Pool()
p1.map(test, listOfArguments)
```
The 'submit' will return a Task if successful. The Task class inherits Event and can be waited on. Once the worker
finishes the task it will set the task. Task also saves the output of the function the task is associated with. 
```python
import time
from PyThreadingPool.ThreadingPool import Pool
p1 = Pool()
t1 = p1.submit(test, "arg", example="kwargs")
while not t1.isSet():
    time.sleep(0.1)
print(f'The task is finish. Results: {t1.results}')
```