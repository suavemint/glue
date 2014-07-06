import time

def timeit(method):
    def timed(*args, **kwargs):
        """
        Note to self: to effect a decorator, make sure to return the
        inputted method.
        """
        
        start_time = time.time()
        result = method(*args, **kwargs)
        end_time = time.time()

        print '%r (%r, %r) %.2f seconds' % ( method.__name__, args, kwargs, end_time - start_time )
        return result
    return timed
