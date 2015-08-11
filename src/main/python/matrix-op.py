import local as local
import numpy
import time

LAT_NAMES = ['x', 'rlat', 'rlats', 'lat', 'lats', 'latitude', 'latitudes']
LON_NAMES = ['y', 'rlon', 'rlons', 'lon', 'lons', 'longitude', 'longitudes']
TIME_NAMES = ['time', 'times', 'date', 'dates', 'julian']


KNMI_PATH="/Users/marroqui/IdeaProjects/SciSparkTestExperiments/src/resources/compData/AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"
# KNMI_PATH="http://acdisc.sci.gsfc.nasa.gov/opendap/ncml/Aqua_AIRS_Level3/AIRH3STD.005/2003/AIRS.2003.01.01.L3.RetStd_H001.v5.0.14.0.G07285113200.hdf.ncml"
WRF_PATH="/Users/marroqui/IdeaProjects/SciSparkTestExperiments/src/resources/compData/AFRICA_UC-WRF311_CTL_ERAINT_MM_50km-rg_1989-2008_tasmax.nc"

""" Step 1: Load Local NetCDF Files into OCW Dataset Objects """
print("Loading %s into an OCW Dataset Object" % (KNMI_PATH,))

knmi_dataset = local.load_file(KNMI_PATH, "tasmax")
wrf_dataset = local.load_file(WRF_PATH, "tasmax")

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
    matrix_elem_wise(m1,m2)
    end = time.time()
    print end  - start

def drange(start, stop, step):
    while start < stop:
        yield start
        start += step

for i in drange(1000, 100000, 1000):
    # print i
    specific_op(i, i)