# To find the spectral coherency of a BIG data Matrix/2D array 
# Spectral coherence has found by using "myFunc"
# Date created: Nov. 22, 2021

import numpy as np
from scipy import signal
from mpi4py import MPI
import h5py as t

chunk_len = 5000 # No. of rows of a matrix
num_c = 34    # No. of column of the matrix

# Actual Dataset
data_mat = np.random.random((10000, num_c))

shape = (chunk_len, data_mat.shape[1])
chunk_size = (chunk_len, 1)
no_of_chunks = data_mat.shape[1]

with t.File('file_name.h5', 'w') as hf:
    hf.create_dataset("chunked_arr",  data=data_mat, chunks=chunk_size, compression='lzf')
del data_mat

def myFunc(dset_X, dset_Y):
    ..............
    ............
    return Real_coh

res = np.zeros((num_c, num_c))

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

for i in range(num_c):
    with t.File('file_name.h5', 'r', libver='latest') as hf:
        dset_X = hf['chunked_arr'][:, i]  # Chunk data reading
    if i % size == rank:
        for j in range(num_c):
            with t.File('file_name.h5', 'r', libver='latest') as hf:
                dset_Y = hf['chunked_arr'][:, j] # Chunk data reading
            res[i][j] = spac(dset_X, dset_Y)
comm.Barrier()
print('Shape of final result :', res.shape )