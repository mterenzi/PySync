from threading import Lock
import server

class File_Thread_Locker:
    """
    Handles Locking threads when interacting with specific files and directories
    to prevent race conditions.
    """
    
    def __init__(self, abspath):
        """
        Initializes Thread Locker context handler.

        Args:
            abspath (str): Absolute path of file being interacted with.
        """
        self.__abspath = abspath
        
    def __enter__(self):
        """
        Acquires lock from global thread locker dictionary and indicates use.
        """
        if self.__abspath in server.thread_locks:
            server.thread_locks[self.__abspath][0] += 1
            server.thread_locks[self.__abspath][1].acquire(timeout=60)
        else:
            server.thread_locks[self.__abspath] = [1, Lock()]
            server.thread_locks[self.__abspath][1].acquire()
        
    def __exit__(self, exc_type, exc_value, trace):
        """
        Releases lock from global thread locker and marks as not in use.
        """
        status = server.thread_locks[self.__abspath]
        status[1].release()
        status[0] -= 1
        server.thread_locks[self.__abspath] = status
