#! /usr/bin/env python
# -*- coding:utf8 -*-

import time
import ray
import tools
from multiprocessing import Pool

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
    
class ProcessRunner():
    """
    异步进程执行框架
    """

    def __init__(self, pool_size, is_async=True):
        print('Init process runner for {0}'.format(pool_size))
        self._pool_size = pool_size
        self._is_async = is_async
        self._ps = Pool(self._pool_size)
        self._results = []

    def execute(self, runner, args=()):
        if self._is_async:
            # print('Execute job with async mode')
            self._results.append(self._ps.apply_async(runner, args=args))
        else:
            print('Execute job with sync mode')
            return self._ps.apply(runner, args=args)

    def close(self):
        print('Close process runner pool')
        self._ps.close()
        self._ps.join()

    def get_results(self):
        return self._results
    
def worker(arg1, arg2):
    print(arg1 + '|' + arg2)
    time.sleep(10)
    return arg1 + '|' + arg2
    
if __name__ == '__main__':
    # tools.run_with_timecost(sequence)
    # tools.run_with_timecost(parallel)
    
    runner = ProcessRunner(5)
    runner.execute(worker, args=('a', 'A'))
    runner.execute(worker, args=('b', 'B'))
    runner.execute(worker, args=('c', 'C'))
    runner.execute(worker, args=('d', 'D'))
    runner.execute(worker, args=('e', 'E'))
    runner.execute(worker, args=('f', 'F'))
    results = runner.get_results()
    for result in results:
        print(result.get())
    runner.close()
    
