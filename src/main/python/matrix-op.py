import local as local
import time

LAT_NAMES = ['x', 'rlat', 'rlats', 'lat', 'lats', 'latitude', 'latitudes']
LON_NAMES = ['y', 'rlon', 'rlons', 'lon', 'lons', 'longitude', 'longitudes']
TIME_NAMES = ['time', 'times', 'date', 'dates', 'julian']


KNMI_PATH="/Users/marroqui/IdeaProjects/SciSparkTestExperiments/src/resources/data/AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"
WRF_PATH="/Users/marroqui/IdeaProjects/SciSparkTestExperiments/src/resources/data/AFRICA_UC-WRF311_CTL_ERAINT_MM_50km-rg_1989-2008_tasmax.nc"

""" Step 1: Load Local NetCDF Files into OCW Dataset Objects """
print("Loading %s into an OCW Dataset Object" % (KNMI_PATH,))
knmi_dataset = local.load_file(KNMI_PATH, "tasmax")
wrf_dataset = local.load_file(WRF_PATH, "tasmax")

start = time.time()
# print knmi_dataset.values[0]
# print wrf_dataset.values[0]
# knmi_dataset.lats - wrf_dataset.lats
# knmi_dataset.lons - wrf_dataset.lons
# knmi_dataset.times - wrf_dataset.times
result = knmi_dataset.values - wrf_dataset.values
end = time.time()
print end  - start