import numpy
import time

def owc():
    for i in range(1,240):
        start = time.time()
# print knmi_dataset.values[0]
# print wrf_dataset.values[0]
# knmi_dataset.lats - wrf_dataset.lats
# knmi_dataset.lons - wrf_dataset.lons
# knmi_dataset.times - wrf_dataset.times
        result = knmi_dataset.values[i] - wrf_dataset.values[i]
# print result
        end = time.time()
        print end  - start

def matrix_mul(m1, m2):
    m1_t = m1.T
    # m3 = m2 * m1
    m3 = numpy.dot(m2, m1_t)

def matrix_elem_wise(m1, m2):
    m3 = m2 * m1

def specific_op(rows, cols):

    m1 = numpy.zeros(shape=(rows,cols))
    m2 = numpy.zeros(shape=(rows,cols))+1

    start = time.time()
    matrix_mul(m1, m2)
    end = time.time()
    print end  - start

def drange(start, stop, step):
    while start < stop:
        yield start
        start += step

for i in drange(1000, 100000, 1000):
    # print i
    specific_op(i, i)