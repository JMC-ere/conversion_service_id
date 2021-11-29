import multiprocessing
from os import getpid
import time
import itertools

def worker(procnum, cc):
    print('I am number %d in process %d' % (procnum, getpid()))
    cc["c"] += 1
    time.sleep(5)
    return getpid()



if __name__ == '__main__':
    mn = multiprocessing.Manager()
    aa = mn.dict()
    aa["c"] = 0
    print(aa)
    pool = multiprocessing.Pool(processes = 3)
    #cc = [{"procnum":1,"bb":2},[2,3], [3,4],[4,5], [5,6],[6,7]]
    #print(cc)
    for i in range(5):
        pool.apply_async(worker, args=(i, aa) )

    pool.close()
    pool.join()
    print(aa)