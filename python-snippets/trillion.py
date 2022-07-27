import time
import numpy as np
import dask.array as da

# Genrate a random numbers array of size 1 Trillion for Numpy Array.
# start = time.time()
# np.random.randint(0, 100000, (1000000000))
# print("Time Taken: "+str(time.time()-start))

# Genrate a random numbers array of size 1 Trillion for Dask Array.
start = time.time()
da.random.random((0, 100000), chunks=(1000000000))
end = time.time()
print("Time Taken: "+str(end-start))
