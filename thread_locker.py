from threading import Lock
import server

class Thread_Locker:
    
    def __init__(self, abspath):
        self.__abspath = abspath
        
    def __enter__(self):
        if self.__abspath in server.thread_locks:
            server.thread_locks[self.__abspath][0] += 1
            server.thread_locks[self.__abspath][1].acquire(timeout=60)
        else:
            server.thread_locks[self.__abspath] = [1, Lock()]
            server.thread_locks[self.__abspath][1].acquire()
        
    def __exit__(self, exc_type, exc_value, trace):
        status = server.thread_locks[self.__abspath]
        status[1].release()
        status[0] -= 1
        server.thread_locks[self.__abspath] = status
