import sys
import numpy as N


def readMERGpixelFile(path, shape=(2, 9896, 3298), offset=75.):
    '''Read MERG brightness temperature from binary file.
File contains two large arrays (2 time epochs: on the hour and the half hour)
of temperature (Kelvin) as an unsigned integer byte, offset by 75 so it will fit in the 0-255 range.
For documentation, see http://www.cpc.ncep.noaa.gov/products/global_precip/html/README.
    '''
    f = open(path, 'rb')
    # i = 0
    # for line in f:
    #     if i > 100: break
    #     print line
    #     i += 1
    x = N.fromfile(f, dtype=N.uint8, count=-1)  # count=-1 means read entire file

    print x.size
    f.close()
    t = x.astype(N.float).reshape(shape)
    t += offset

    lon = N.arange(0.0182, 360., 0.036378335, dtype=N.float)
    print lon.shape
    print lon
    lat = N.arange(59.982, -60., -0.036383683, dtype=N.float)
    print lat.shape
    print lat
    print t.shape
    return (lon, lat, t)


if __name__ == '__main__':
    lon, lat, t = readMERGpixelFile(sys.argv[1])
    print t
