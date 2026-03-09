import os
import time

from decorator import decorator

# Adapted from https://stackoverflow.com/questions/29344366/lock-file-for-access-on-windows


class AlreadyLocked(Exception):
    pass


class LockFile:
    def __init__(self, fname):
        self.fname = fname

    def lock(self):
        try:
            with open(self.fname, 'x') as lockfile:
                # write the PID of the current process so you can debug
                # later if a lockfile can be deleted after a program crash
                lockfile.write(str(os.getpid()))
        except IOError:
            # file already exists
            raise AlreadyLocked()

    def unlock(self):
        os.remove(self.fname)


@decorator    
def do_with_lock(func, lockfile='lock.file', timeout=100, check_interval=5, *args, **kw):
    """
    executes function with a system wide exclusive lock on lockfile and total timeout for waiting with check interval 

    To use this as a decorator ::

        @do_with_lock(lockfile='my.test.lock',timeout=20,check_interval=5)
        def my_func(...):
            pass

        or 

        do_with_lock(lockfile='my.test.lock',timeout=20,check_interval=1)(my_func(...))

    Parameters
    ----------
    func : callable function
        a function or other callable being decorated
    lockfile : str, optional
       path to lock file, by default 'lock.file'
    timeout : in seconds int, optional
        total timeout in seconds, by default 100 seconds
    check_interval :check interval in seconds, optional
        check interval till total timeout expires, by default 5s

    Returns
    -------
        value of wrapped function

    Raises
    ------
    TimeoutError
        if the total timeout expires
    """    
    lf = LockFile(lockfile)
    while True:
        try:
            lf.lock()
            try:
                return func(*args, **kw)
            finally:
                lf.unlock()
                break
        except AlreadyLocked as exc:
            time.sleep(check_interval)
            timeout -= check_interval
            if timeout > 0:
                continue
            else:
                raise TimeoutError(f'Lockfile not available even after timeout of {timeout} seconds!')
