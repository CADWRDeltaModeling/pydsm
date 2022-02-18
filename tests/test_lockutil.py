'''
Test function for lockutil
'''
import os
from pydsm import lockutil

def test_lock():

    def my_func():
        assert os.path.exists('my.func.lock')
        import time
        time.sleep(5)

    assert not os.path.exists('my.func.lock')
    lockutil.do_with_lock(my_func, lockfile='my.func.lock', timeout=10, check_interval=5)()
    assert not os.path.exists('my.func.lock')

