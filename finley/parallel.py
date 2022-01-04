#! /usr/bin/env python
# -*- coding:utf8 -*-

import time
import ray
import tools

ray.init()

def f1():
    time.sleep(1)
    
@ray.remote    
def f2():
    time.sleep(1)

def sequence():    
    [ f1() for _ in range(10)]
    
def parallel():
    [ f2.remote() for _ in range(10)]
    
if __name__ == '__main__':
    tools.run_with_timecost(sequence)
    tools.run_with_timecost(parallel)
    
