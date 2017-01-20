# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import getopt
import glob
import json
import numpy
import os
import shutil
import subprocess
import sys
import urllib
import zipfile
from collections import Counter
from datetime import datetime, timedelta
from itertools import groupby
from netCDF4 import Dataset

def _get_python_implementation():
    '''
    Purpose: To get the results of python implementation from the SciSpark website
    Inputs: None
    Outputs: Boolean representing success retrieving the data
    '''
    print 'Checking on Python implementation data'
    of.write('\n Checking on Python implementation data')

    pythonGTG = 'https://scispark.jpl.nasa.gov/pythonGTG.zip'

    os.chdir(workingDir)
    try:
        if not os.path.exists(workingDir + '/pythonGTG'):
            urllib.urlretrieve(pythonGTG, workingDir + '/pythonGTG.zip')
            zipGTG = zipfile.ZipFile(workingDir + '/pythonGTG.zip', 'r')
            zipGTG.extractall(workingDir)
            zipGTG.close()
            os.remove(workingDir + '/pythonGTG.zip')

        print 'Python implementation data retrieved from %s' %pythonGTG
        of.write('Python implementation data retrieved from %s' %pythonGTG)
        return True
    except:
        print '!!Problem retrieving %s' %pythonGTG
        of.write('\n!!Problem retrieving %s' %pythonGTG)
        return False

def _run_scispark_implementation():
    '''
    Purpose: To run GTG runner for SciSpark results to compare as a spark-submit task
    Assumptions: spark-submit is available on the path. SciSpark jar is available at
                  /target/scalaVersion/SciSpark.jar
    '''
    print 'Checking on SciSpark implementation data'
    of.write('\n Checking on SciSpark implementation data')

    try:
        if os.path.exists(workingDir + '/scisparkGTG'):
            shutil.rmtree(workingDir + '/scisparkGTG')

        os.mkdir(workingDir + '/scisparkGTG')
        os.mkdir(workingDir + '/scisparkGTG/textFiles')
        os.mkdir(workingDir + '/scisparkGTG/MERGnetcdfCEs')

        os.chdir(workingDir + '/../../../../')

        sparkSubmitStr = 'spark-submit target/' + scalaVersion + '/SciSpark.jar'
        subprocess.call(sparkSubmitStr, shell=True)
        outputDirectory = "output"
        resultDir = max([os.path.join(outputDirectory,d) for d in os.listdir(outputDirectory)], key=os.path.getmtime)

        cpTextFilesStr = 'cp {0}/MCSEdges.txt {1}/scisparkGTG/textFiles'.format(resultDir, workingDir)
        subprocess.call(cpTextFilesStr, shell=True)
        cpTextFilesStr = 'cp {0}/MCSNodes.json {1}/scisparkGTG/textFiles'.format(resultDir, workingDir)
        subprocess.call(cpTextFilesStr, shell=True)
        cpTextFilesStr = 'cp {0}/MCCPaths.txt {1}/scisparkGTG/textFiles'.format(resultDir, workingDir)
        subprocess.call(cpTextFilesStr, shell=True)
        filenames = glob.glob(resultDir + '/subgraphs*.txt')
        with open(workingDir + '/scisparkGTG/textFiles/subgraphs.txt', 'w') as outfile:
            for fname in filenames:
                with open(fname) as infile:
                    outfile.write(infile.read())
        cpNetcdfsStr = 'cp /tmp/*.nc ' + workingDir + '/scisparkGTG/MERGnetcdfCEs'
        subprocess.call(cpNetcdfsStr, shell=True)

        print 'SciSpark implementation successfully run. Data can be found at %s' \
            %workingDir + '/scisparkGTG'
        of.write('\nSciSpark implementation successfully run. Data can be found at %s' \
            %workingDir + '/scisparkGTG')
        return True
    except:
        print '!! Problem running SciSpark MCS implementation!'
        of.write('\n!! Problem running SciSpark MCS implementation!')
        return False

def _compare_times(pyNodes, ssDir, allTimesInts):
    '''
    Purpose: To check the times of files in two nodelist to determine if similar
    Inputs: pyNodes - a list of strings (F##CE##) representing the nodes found in the Python
                    implementation. F## is the frame integer
            pyDir - string representing the path to the directory with the netCDF files from the
                    Python implementation
            allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime & endTime
    Assumptions: Files contain a datetime string
    '''
    print 'Checking the times in the files ...'
    ssFiles = glob.glob(ssDir + '/MERGnetcdfCEs/*.nc')
    ssTimes = sorted(set(map(lambda x: int(x.split('/')[-1].split('CE')[0].split('F')[1]), ssFiles)))
    pyTimes = sorted(set(map(lambda x: allTimesInts[int(x.split('F')[1].split('CE')[0]) -1], pyNodes)))

    if pyTimes == ssTimes:
        return True, (pyTimes, ssTimes)
    else:
        return False, (pyTimes, ssTimes)

def _compare_num_CEs_per_frame(pyNodes, ssNodes, allTimesInts):
    '''
    Purpose: To compare the number of number of CEs per a frame
    Inputs: pyNodes - a list of strings (F##CE##) representing the nodes found in the Python
                    implementation. F## is the frame integer
            ssNodes - a list of strings representing the nodes found in the SciSpark implementation.
                    F## is the datetime of the frame in the format YYYYMMDDhh
            allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime & endTime
    Outputs: CEs - list of tuples showing the # of CEs found at each frame for either implementation
    Assumptions: Files contain a datetime string
    '''
    print 'Checking the number of CEs at each frame ...'
    CEs = []

    for t in range(len(allTimesInts)):
        pFiles = sorted(filter(lambda x: x.split('F')[1].split('CE')[0] == str(t + 1), pyNodes))
        sFiles = sorted(filter(lambda x: x.split('F')[1].split('CE')[0] == str(allTimesInts[t]), ssNodes))

        if len(pFiles) == len(sFiles):
            CEs.append((allTimesInts[t], len(pFiles)))
        elif len(pFiles) > len(sFiles):
            CEs.append((('python: F' + str(t + 1), len(pFiles)), ('scispark:' + str(allTimesInts[t]), len(sFiles))))
        elif len(pFiles) < len(sFiles):
            CEs.append((('python: F' + str(t + 1), len(pFiles)), ('scispark:' + str(allTimesInts[t]), len(sFiles))))

    if len(CEs) == 0:
        return True, CEs
    else:
        return False, CEs

def _compare_content_in_CEs(pyDir, ssDir, pyNodes, allTimesInts):
    '''
    Purpose: Determine the overlap between the CEs for each frame from the two implementations
    Inputs: pyDir - string representing the path to the directory with the netCDF files from the
                    Python implementation
            ssDir - string representing the path to the directory with the netCDF files from the
                    SciSpark implementation
            pyNodes - a list of strings (F##CE##) representing the nodes found in the Python
                    implementation. F## is the frame integer
            allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime and endTime
    Outputs: CEs - a list of list of tuples [frame, (python_CE, scispark_CE, overlap, %overlap if a float,
                            or number of pts not overlapping when one node is a subset of another)]
    Assumptions: Files contain a datetime string
    '''

    print 'Comparing the content for the CEs at each frame ...'

    CEs = []
    pyTimes = sorted(set(map(lambda x: allTimesInts[int(x.split('F')[1].split('CE')[0]) - 1], pyNodes)))

    for t in pyTimes:
        pyData = []
        ssData = []
        maxOverlapFrame = []
        alldiff = []
        checkedSSCEs = []

        pyFormat = str(t)[:4] + '-' + str(t)[4:6] + '-' + str(t)[6:8] + '_' + str(t)[8:10] + ':00:00'

        # get files associated with what is in the nodelist only
        pC = filter(lambda x: int(x.split('F')[1].split('CE')[0]) == allTimesInts.index(t) + 1, pyNodes)
        pyAllCEs = glob.glob(pyDir + '/MERGnetcdfCEs/*' + pyFormat + '*.nc')
        pFiles = filter(lambda x: x.split(':00:00')[1].split('.nc')[0] in pC, pyAllCEs)
        pyCEs = map(lambda y: y.split('/')[-1], pFiles)

        sFiles = glob.glob(ssDir + '/MERGnetcdfCEs/*' + str(t) + '*.nc')

        for pyFIdx in range(len(pFiles)):
            f = Dataset(pFiles[pyFIdx], 'r')
            pyData.append(numpy.squeeze(f.variables['brightnesstemp'][:, :, :], axis=0))
            f.close()

        if pyData:
            for ssF in sFiles:
                f = Dataset(ssF, 'r')
                fce = f.variables['CE_brightness_temperature'][:, :]
                ssData.append(fce)
                f.close()

                if pyData[pyFIdx].shape != fce.shape:
                    print '!!!Error: Data shapes are different. Aborting tests. Breaking at %s' %(pFiles[pyFIdx], ssF)
                    sys.exit(1)

            # check equality of the two files (equivalent % of similar values). Assumes Python implementation as truth
            for i in range(len(pyData)):
                for y in range(len(ssData)):
                    # if ssCE is entirely in python CE, mark it as true and note how much larger the python CE is than the ssCE
                    if numpy.count_nonzero(pyData[i] - ssData[y]) == (numpy.count_nonzero(pyData[i]) - numpy.count_nonzero(ssData[y])) \
                       and numpy.count_nonzero(pyData[i] - ssData[y]) > 0:
                        alldiff.append((pFiles[i].split('/')[-1], sFiles[y].split('/')[-1], True, numpy.count_nonzero(pyData[i] - ssData[y])))
                    # check if pyCE is in ssCE
                    else:
                        alldiff.append((pFiles[i].split('/')[-1], sFiles[y].split('/')[-1], numpy.array_equiv(pyData[i], ssData[y]), \
                            (pyData[i] == ssData[y]).sum() * 1.0 / pyData[i].flatten().size))

            # first place all True in maxOverlapFrame, then check for highest %
            for pyCE in pyCEs:
                oneCE = [i for i in alldiff if i[0] == pyCE]
                sortOneCE = sorted(oneCE, key=lambda x: x[3], reverse=True)

                checkedSSCEs = filter(lambda y: y[3] > 1.0, sortOneCE)

                if checkedSSCEs == []:
                    checkedSSCEs.append(sortOneCE[0][1])
                    maxOverlapFrame.append(sortOneCE[0])
                elif checkedSSCEs != [] and maxOverlapFrame == []:
                    [maxOverlapFrame.append(x) for x in checkedSSCEs]
                else:
                    for c in sortOneCE:
                        if not c[1] in checkedSSCEs:
                            maxOverlapFrame.append(c)
                            checkedSSCEs.append(c[1])
                            break

            CEs.append((t, maxOverlapFrame))

    if len(filter(lambda y: y[0][2] == False, map(lambda x: x[1], CEs))) != 0:
        return False, CEs
    else:
        return True, CEs

def _write_CE_mappings(allCEs):
    '''
    Purpose: Indicate the mappings of the pyCEs to the ssCEs
    Inputs: allCEs - a list of list of tuples [frame, (python_CE, scispark_CE, overlap, %overlap if a float,
                            or number of pts not overlapping when one node is a subset of another)]
    Outputs: writes a file in the output directory called CEmappings.txt with the data
    '''
    ceMap = []
    a = map(lambda y: y[1], allCEs)
    for i in a:
        ceMap.extend(map(lambda x: (x[0].split(':00:00')[1].split('.nc')[0], x[1].split('_')[0]), i))

    try:
        with open(workingDir + '/CEmappings.txt', 'w') as f:
            for i in ceMap:
                f.write(i[0] + ' --> ' + i[1] + '\n')

        of.write('\nWrote the CE mappings at ' + workingDir + '/CEmappings.txt \n')
    except:
        of.write('\n!! Problem with writing CEmappings to file \n')
        print '!! Problem with writing CEmappings'

def _graph_from_edgeList(ssEdgeList):
    '''
    Return the graph (represented a list of lists (subgraphs)) from a edgelist generated by the SciSpark implementation
    Inputs: ssEdgeList - a list of tuples of two strings representing an edge between connected nodes with subgraphs
    Outputs: subgraphs - a list of lists of strings representing nodes that are connected between frames
    '''
    groupededges = {}
    nodes = set()
    nodesArray = []
    subgraphs = []
    groups = []
    seenNodes = []
    nodeGraphs = {}

    for i in ssEdgeList:
        nodes.add(i[0])
        nodes.add(i[1])

    nodesArray = [i for i in nodes]

    for key, group in groupby(ssEdgeList, lambda x: x[0]):
        groupededges[key] = [k for k in group]

    for currNode in nodesArray:
        nodeGraphs[currNode] = _cloud_cluster(currNode, groupededges)

    for k, v in nodeGraphs.iteritems():
        if k not in seenNodes:
            if not set(sum(v, ())) & set(sum(subgraphs, [])):
                subgraphs.append(list(sum(v, ())))
                seenNodes.extend(list(sum(v, ())))
                seenNodes = list(set(seenNodes))
            else:
                seenNodes.extend(list(sum(v, ())))
                seenNodes = list(set(seenNodes))
                for c in range(len(subgraphs)):
                    if set(sum(v, ())) & set(subgraphs[c]):
                        subgraphs[c].extend(list(sum(v, ())))
                        subgraphs[c] = list(set(subgraphs[c]))
                        break
    return subgraphs

def _convert_to_pyNodes(subgraphs, workingDir):
    '''
    Return the graph (represented a list of lists (subgraphs)) from a edgelist generated by the SciSpark
          implementation with Python implementation node names
    Inputs: subgraphs - a graph represented as a list of list of strings representing the nodes in each (sub)graph
            workingDir - - directory for writing mapping files
    Outputs: ssGraph - list of list of strings representing the graphs as a list of nodes with Python
            implmentation naming convention
    '''
    with open(workingDir + '/CEmappings.txt', 'r') as f:
        CEmappings = f.readlines()

    CEmap = map(lambda x: (x.strip().split(' --> ')[0], x.strip().split(' --> ')[1]), CEmappings)
    mappedGraphs = []
    ssGraph = []

    for i in subgraphs:
        pySub = []
        for j in i:
            x = filter(lambda z: j in z[1], CEmap)
            if x: pySub.append('F'+x[0][0].split('F')[1].split('.nc')[0])
        ssGraph.append(sorted(pySub, key=lambda x: x.split('F')[1].split('CE')[0]))

    return ssGraph

def _compare_num_subgraphs(pyEdgeList, ssEdgeList):
    '''
    Purpose: Compare the number of subgraphs generated between the two implementations
    Inputs: pyEdgeList - a list of list of strings representing the connect nodes within each subgraph
            ssEdgeList - a list of tuples of two strings representing an edge between connected nodes with subgraphs
    Outputs: Boolean representing if the two list entered are equal, a list of list representing any difference between the inputs
    '''
    if len(pyEdgeList) == len(ssEdgeList):
        return True, 0
    elif len(pyEdgeList) > len(ssEdgeList):
        return False, len(pyEdgeList) - len(ssEdgeList)
    elif len(ssEdgeList) > len(pyEdgeList):
        return False, (len(ssEdgeList) - len(pyEdgeList)) * -1


def _compare_edgelists(pySubgraphs, ssSubgraphs):
    '''
    Purpose: Compare the edgelist generated between the two implementations
    Inputs: pySubgraphs - a list of list of strings representing the connect nodes within each subgraph
            ssSubgraphs - a list of tuples of two strings representing an edge between connected nodes with subgraphs
    Outputs: Boolean representing if the two list entered are equal, a list of list representing any difference between the inputs
    '''
    x = []
    z = []

    for i in pySubgraphs:
        ssGraph = filter(lambda y: set(y) & set(i), ssSubgraphs)
        if not ssGraph:
            x.append(filter(lambda y: set(y) & set(i) != set([]), ssSubgraphs))
        else:
            if len(i) > len(ssGraph[0]):
                z.append(('Python len: ' + str(len(i)), 'SciSpark len: '+str(len(ssGraph[0])), list(set(i) - set(ssGraph[0]))))
            elif len(ssGraph[0]) > len(i):
                z.append(('Python len: ' + str(len(i)), 'SciSpark len: '+str(len(ssGraph[0])), list(set(ssGraph[0]) - set(i))))
            else:
                z.append(('Python len: '+str(len(i)), 'SciSpark len: '+str(len(ssGraph[0])), []))
    if x:
        return False, x
    else:
        return True, z

def _cloud_cluster(nodeName, groupedges):
    '''
    Purpose: Find the subgraphs
    Inputs: nodeName - a string representing the node
            groupedges - a key:value where values is a list representing all the nodes connected to the key
    Outputs: edgelist
    '''
    edgelist = []
    thisStack = [nodeName]

    while len(thisStack) > 0:
        z = thisStack.pop()
        edges = []

        if groupedges.has_key(z):
            edges = groupedges[z]

        for i in edges:
            edgelist.append(i)
            thisStack.append(i[1])

    return edgelist

def _get_ssSubgraphs(ssDir):
    '''
    Purpose: to acquire the subgraphs found in the SciSpark implmentation from the text file
    Input: ssDir - string representing the path to the directory with the netCDF files from the SciSpark implementation
    Output: ssSubgraphs - a list of subgraphs from the SciSpark implementation
    '''
    subgraphs = []

    with open(ssDir+'/textFiles/subgraphs.txt') as f:
        s = f.readlines()
    subgraphs = map(lambda x: list(set((x.strip().split('Set(')[1].split(')')[0]).replace(' ', '').split(','))), s)

    return subgraphs


def _get_data(sTime, eTime, pyDir, ssDir):
    '''
    Purpose: to acquire necessary data from the two implementations
    Inputs: sTime - int (YYYYMMDDhh) representing the starttime for the input data in the test
            eTime - int (YYYYMMDDhh) representing the end time for the input data in the test
            pyDir - string representing the path to the directory with the netCDF files from the python implementation
            ssDir - string representing the path to the directory with the netCDF files from the SciSpark implementation
    Outputs: allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime and endTime
            pyNodes - a list of strings (F##CE##) representing the nodes found in the Python implementation.
                    F## is the frame integer
            ssNodes - a list of strings representing the nodes found in the SciSpark implementation.
                    F## is the datetime of the frame in the format YYYYMMDDhh
    '''
    startTime = datetime(int(str(sTime)[:4]), int(str(sTime)[4:6]), int(str(sTime)[6:8]), int(str(sTime)[8:10]))
    endTime = datetime(int(str(eTime)[:4]), int(str(eTime)[4:6]), int(str(eTime)[6:8]), int(str(eTime)[8:10]))
    a = [aDate for aDate in [startTime + timedelta(hours=i) for i in xrange(((endTime - startTime).days * 24 \
        + (endTime - startTime).seconds / 3600) + 1)]]
    allTimesInts = map(lambda x: int(x.strftime('%Y%m%d%H')), a)

    with open(ssDir+'/textFiles/MCSNodes.json', 'rb') as sF:
        sFs = sF.readlines()
    ssNodes = map(lambda y: 'F' + str(y['frameNum'])+'CE' + str(y['cloudElemNum'])[:-2], map(lambda x: json.loads(x), sFs))

    with open(pyDir + '/textFiles/CEList.txt', 'r') as pF:
        pFs = pF.readline()
    pyNodes = map(lambda y: y.lstrip(), sorted(pFs[1:-1].replace('\'', '').split(','), key=lambda x: x.split('F')[1].split('CE')[0]))

    with open(ssDir + '/textFiles/MCSEdges.txt', 'r') as sF:
        sFs = sF.readlines()
    ssEList = map(lambda x: x + '))', sFs[0].split('List(')[1][:-3].split(')), '))
    ssEdgeList = map(lambda x: ('F' + x.split(',')[0].split('((')[1].split(':')[0] + 'CE' + x.split(',')[0].split('((')[1].split(':')[1][:-1], \
        'F' + x.split(',')[1].split('(')[1].split(':')[0] + 'CE' + x.split(',')[1].split('(')[1].split(':')[1][:-2]), ssEList)

    with open(pyDir + '/textFiles/MCSList.txt', 'r') as pF:
        pFs = pF.readline()
    pyEdgeList = map(lambda x: x[1:].replace('\'', '').replace(' ', '').split(','), pFs[1:-1].split(']'))

    return allTimesInts, ssNodes, pyNodes, ssEdgeList, pyEdgeList

def _convert_to_pyStr(workingDir, scisparkMCCsList):
    '''
    Purpose: execute the sixth tests to compare the subgraphs generated within either implementation
    Inputs: workingDir - directory for writing mapping files
            scisparkMCCsList - list of subgraphs from SciSpark implementation
    Outputs: mappedGraph - list of subgraphs from SciSpark implementation with nodes in Python implementation format
    '''
    mappedGraph = []
    aGraph = []
    scisparkMCCs = []

    with open(workingDir + '/CEmappings.txt', 'r') as f:
        nodeMap = f.readlines()

    nodeMap = map(lambda x: (x.split(' --> ')[0], x.split(' --> ')[1].strip()), nodeMap)
    
    #convert the scisparkList to the python string format
    for s in scisparkMCCsList:
        scisparkMCCs.append(map(lambda x: 'F' + x.split(':')[0] + 'CE' + x.split(':')[1], filter(None, s)))

    for g in scisparkMCCs:
        for n in g:
            entry = filter(lambda y: y[1] == n, nodeMap)
            if entry:
                pyNode = entry[0][0]
            else:
                pyNode = None
            aGraph.append(pyNode)
        mappedGraph.append(aGraph)
        aGraph = []

    return mappedGraph


def test_1(pyNodes, ssDir, allTimesInts):
    '''
    Purpose: execute the first test to check the times within either implementation
    Inputs: pyNodes - a list of strings (F##CE##) representing the nodes found in the Python
                    implementation. F## is the frame integer
            ssDir - string representing the path to the directory with the netCDF files from 
                    the SciSpark implementation
            allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime & endTime
    Outputs: None
    '''
    passed, runTimes = _compare_times(pyNodes, ssDir, allTimesInts)
    if passed:
        print 'Test 1: The times between implementations are similar'
        of.write('\nTest 1: The times between implementations are similar \n')
        if len(allTimesInts) != len(runTimes[0]):
            print '\t* Note: there are no CEs in either implementation at these times: %s' \
                %sorted(list(set(allTimesInts) - (set(allTimesInts) & set(runTimes[0]))))
            of.write('\t* Note: there are no CEs in either implementation at these times: %s \n' \
                %sorted(list(set(allTimesInts) - (set(allTimesInts) & set(runTimes[0])))))
    else:
        print '!!Test 1: The times between implementations are NOT similar. \npyDir times are %s.\nssDir times are %s.' \
            %(runTimes[0], runTimes[1])
        print '\t*Note: there are no CEs in Python implementations at these times: %s' \
            %list(set(allTimesInts) - (set(allTimesInts) & set(runTimes[0])))
        print '\t*Note: there are no CEs in SciSpark implementation at these times: %s' \
            %list(set(allTimesInts) - (set(allTimesInts) & set(runTimes[1])))
        of.write('\n!!Test 1: The times between implementations are NOT similar. \npyDir times are %s.\nssDir times are %s' \
            %(runTimes[0], runTimes[1]))
        of.write('\t*Note: there are no CEs in Python implementations at these times: %s' \
            %list(set(allTimesInts) - (set(allTimesInts) & set(runTimes[0])))+'\n')
        of.write('\t*Note: there are no CEs in SciSpark implementation at these times: S3' \
            %list(set(allTimesInts) - (set(allTimesInts) & set(runTimes[1])))+'\n')


def test_2(allTimesInts, pyNodes, ssNodes):
    '''
    Purpose: execute the 2nd test to compare number of CEs at each frame
    Inputs: allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime & endTime
            pyNodes - a list of strings (F##CE##) representing the nodes found in the
                    Python implementation. F## is the frame integer
            ssNodes - a list of strings representing the nodes found in the SciSpark implementation.
                   F## is the datetime of the frame in the format YYYYMMDDhh
    Outputs: None
    '''
    pyCEs = 0
    ssCEs = 0

    passed, CEs = _compare_num_CEs_per_frame(pyNodes, ssNodes, allTimesInts)

    for i in CEs:
        try:
            if 'python' in i[0][0]:
                pyCEs += i[0][1]
            if 'scispark' in i[1][0]:
                ssCEs += i[1][1]
        except:
            pyCEs += i[1]
            ssCEs += i[1]

    if passed:
        print 'Test 2: Number of CEs at each frame are similar. There are %d CEs in total. ' %pyCEs
        of.write('\nTest 2: Number of CEs at each frame are similar. There are  '+ pyCEs + 'CEs in total. ')
    else:
        print '!!Test 2: Different number of CEs at each frames. There are %d CEs in the Python implementation and %d CEs \
            in the SciSpark implementation. \nPlease check! (implementation: frame, #_of_CEs) \n %s' \
            %(pyCEs, ssCEs, filter(lambda x: 'python' in str(x[0]), CEs))
        of.write('\n!!Test 2: Different number of CEs at each frames. There are %d CEs in the Python implementation and %d CEs\
            in the SciSpark implementation. \nPlease check! (implementation: frame, #_of_CEs) \n %s\n' \
            %(pyCEs, ssCEs, filter(lambda x: 'python' in str(x[0]), CEs)))
        of.write('*'*25)
        of.write('\n All CEs at times are %s\n' %str(CEs))

def test_3(pyDir, ssDir, pyNodes, allTimesInts):
    '''
    Purpose: execute the third test to check the times within either implementation
    Inputs: pyDir - string representing the path to the directory with the netCDF files from the Python implementation
            ssDir - string representing the path to the directory with the netCDF files from the SciSpark implementation
            pyNodes - a list of strings (F##CE##) representing the nodes found in the Python implementation. F## is the frame integer
            allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime and endTime
    Outputs: allCEs - a list of list of tuples [frame, (python_CE, scispark_CE, overlap, %overlap if a float,
                            or number of pts not overlapping when one node is a subset of another)]
    '''
    accounted = []
    accCeMap = []
    passed, allCEs = _compare_content_in_CEs(pyDir, ssDir, pyNodes, allTimesInts)
    if passed:
        print 'Test 3: Content of CEs at each frame are similar.'
        of.write('\nTest 3: Content of CEs at each frame are similar.')
    else:
        print '!!Test 3: Different content of CEs at each frames.'
        of.write('\n!!Test 3: Different content of CEs at each frames.')
        ceMap = []
        a = map(lambda y: y[1], allCEs)
        for i in a:
            ceMap.extend(filter(lambda x: x[2] == True, i))
        print '!! %d CEs are accounted for.' %len(ceMap)
        of.write('\n!! ' + str(len(ceMap))+' CEs are accounted for.')
        # if accounted for CEs > len(pyNodes) then there were multiple nodes in scispark implementation for one node.
        if len(ceMap) > len(pyNodes):
            accounted = map(lambda x: x[0].split(':00:00')[1].split('.nc')[0], ceMap)
            accCeMap = ceMap

        ceMap = []
        a = map(lambda y: y[1], allCEs)
        for i in a:
            ceMap.extend(filter(lambda x: x[2] == False, i))
        print '!! %d CEs are unaccounted for. They are: \n %s' %(len(ceMap), ceMap)
        of.write('\n!! ' + str(len(ceMap))+' CEs are unaccounted for. They are: %s \n' %str(ceMap))
        of.write('*'*25)
        print '*'*25

        if accounted:
            c = Counter(accounted) - Counter(pyNodes)
            print '%s in the Python implementation represented by multiple CEs in SciSpark implementation.' %c.keys()
            of.write('\n%s in the Python implementation represented by multiple CEs in SciSpark implementation. \n' %c.keys())
            for ce in c.keys():
                print '%s has %d CEs in SciSpark implementation. Namely: %s' \
                    %(ce, c.get(ce) + 1, map(lambda j: j[1], filter(lambda i: ce in i[0], accCeMap)))
                of.write('%s has %d CEs in SciSpark implementation. Namely: %s \n' \
                    %(ce, c.get(ce) + 1, map(lambda j: j[1], filter(lambda i: ce in i[0], accCeMap))))
    return allCEs

def test_4(pySubgraphs, ssEdgeList):
    '''
    Purpose: execute the fourth tests to compare if the edgelist generated at the end of step one contains the graphs
    Inputs: pySubgraphs - a list of list of strings representing the connect nodes within each subgraph
            ssEdgeList - a list of tuples of two strings representing an edge between connected nodes with subgraphs
    Outputs: None
    '''
    subgraphs = _graph_from_edgeList(ssEdgeList)
    subgraphs = filter(lambda x: len(list(set(map(lambda y: int(y.split('F')[1].split('CE')[0]), x)))) > 3, subgraphs)
    subgraphs = map(lambda y: list(set(y)), subgraphs)
    ssgraph = _convert_to_pyNodes(subgraphs, workingDir)

    passed, graph = _compare_edgelists(pySubgraphs, ssgraph)
    if passed:
        print 'Test 4: Graph objects generated are similar.'
        of.write('\nTest 4. Graph objects generated are similar.')
    else:
        print '!!Test 4: Different graph objects are generated in either implementation. \n \
        The %d graphs in Python implementation and %d in SciSpark implementation. The different subgraphs are %s' \
            %(len(pySubgraphs), len(ssgraph), graph)
        of.write('\n!!Test 3: Different graph objects are generated in either implementation.\n\
            The %d graphs in Python implementation and %d in SciSpark implementation. The different subgraphs are %s'\
            %(len(pySubgraphs), len(ssgraph), graph))


def test_5(pySubgraphs, ssSubgraphs):
    '''
    Purpose: execute the fifth tests to compare the subgraphs generated within either implementation
    Inputs: pyEdgeList - a list of list of strings representing the connect nodes within each subgraph
            ssEdgeList - a list of tuples of two strings representing an edge between connected nodes with subgraphs
    Outputs: None
    '''
    passed, diffLen = _compare_num_subgraphs(pySubgraphs, ssSubgraphs)
    if passed:
        print 'Test 5: Number of subgraphs generated are equal.'
        of.write('\nTest 5. Number of subgraphs generated are equal.')
    else:
        if diffLen > 0:
            print 'Python implementation found %d more subgraphs than the SciSpark implementation' %diffLen
            of.write('\nPython implementation has found %d more subgraphs than the SciSpark implementation' %diffLen)
        else:
            print 'SciSpark implementation found %d more subgraphs than the Python implementation' %(diffLen * -1)
            of.write('\nSciSpark implementation has found %d more subgraphs than the Python implementation' %(diffLen * -1))

def test_6(workingDir, pySubgraphs, ssSubgraphs):
    '''
    Purpose: execute the sixth tests to compare the subgraphs generated within either implementation
    Inputs: workingDir - - directory for writing mapping files
    pySubgraphs - a list of list of strings representing the connect nodes within each subgraph
            ssSubgraphs - a list of tuples of two strings representing an edge between connected nodes with subgraphs
    Outputs: None
    '''

    mappedGraph = []
    aGraph = []
    ssgraph = []

    mappedGraph = _convert_to_pyStr(workingDir, ssSubgraphs)
    passed, graph = _compare_edgelists(pySubgraphs, mappedGraph)

    if passed:
        print 'Test 6: Subgraphs generated are similar. %s' %graph 
        of.write('\nTest 6. Subgraphs generated are similar.')
    else:
        print '!!Test 6: Different elements in the subgraphs are generated in either implementation. \n \
        The different for each subgraph are %s' \
            %graph
        of.write('\n!!Test 6: Different elements in the subgraph are generated in either implementation.\n\
            The difference for each subgraph are %s'\
            %graph)

def test_7(workingDir, pyDir, ssDir):
    '''
    Purpose: execute the seventh tests to compare the MCCs found within either implementation
    Inputs: workingDir - - directory for writing mapping files
    Outputs: None
    '''
    with open(pyDir + '/textFiles/MCCList.txt', 'r') as p:
        pyMCCs = p.readlines()
    pyMCCsList = map(lambda x: x[2:-2].split("', '"), pyMCCs)

    with open(ssDir + '/textFiles/MCCPaths.txt', 'r') as s:
        ssMCCs = s.readlines()
    ssMCCsList = map(lambda x: x[4:-1].split(', '), ssMCCs)

    mappedMCCs = _convert_to_pyStr(workingDir, ssMCCsList)
    passed, graph = _compare_edgelists(pyMCCsList, mappedMCCs)

    if passed:
        print 'Test 7: MCCs generated are similar. %s' %graph 
        of.write('\nTest 6. Subgraphs generated are similar.')
    else:
        print '!!Test 7: Different elements in the MCCs are generated in either implementation. \n \
        The different for each MCC are %s' \
            %graph
        of.write('\n!!Test 7: Different elements in the MCCs are generated in either implementation.\n\
            The difference for each MCC are %s'\
            %graph)


def main(argv):
    '''
    Assumes outputs from either implementation are available. Args to dirs should point to the top level of the dir,
            assuming there is:
            /MERGnetcdfCEs/ with CEsNetcdfs
            /textFiles/ with any outputs necessary for either implementation e.g. file of node names
    '''
    global of
    global workingDir
    global scalaVersion
    sTime = 2006091100
    eTime = 2006091212
    # create folder for outputs
    if not glob.glob(os.getcwd() + '/verification'): os.mkdir('verification')

    workingDir = (os.getcwd() + '/verification')
    pyDir = workingDir + '/pythonGTG'
    ssDir = workingDir + '/scisparkGTG'

    try:
        opts, arg = getopt.getopt(argv, "hs:")

        if len(opts) == 0:
            print 'Please run: python MCSVerification.py -h'
            sys.exit(2)

        for opt, arg in opts:
            if '-h' in opt: 
                print 'These are the verification test to determine if SciSpark GTG is operating correctly.'
                print 'To run: python MCSVerification.py -s scalaVersion'
                print 'For example: python MCSVerification.py -s scala-2.11'
                print 'The data used in the runs were used in the paper, and the python GTG results are downloaded.'
                print 'If needed, the original GTG can de downloaded and used to generate the results. \
                    Please use the original release - Release 1.0'
                sys.exit(2)
            elif '-s' in opt: 
                scalaVersion = arg
                if not os.path.exists('../../../target/' + scalaVersion):
                    print '!! scalaVersion Scispark jar not found'
                    sys.exit(2)
            else:
                print 'Please run: python MCSVerification.py -h'
                sys.exit(2)
    except getopt.GetoptError:
        print 'Please run: python MCSVerification.py -h'
        sys.exit(2)

    print 'Starting MCS accuracy tests ...'
    print 'Using Python implementations results at %s' %pyDir
    print 'Using SciSpark implementation results at %s' %ssDir
    print 'Results will be stored at %s in %s' %(workingDir, 'output.log')

    with open(workingDir + '/output.log', 'w') as of:
        of.write('Starting MCS accuracy tests ...\n')
        of.write('Using Python implementations results at ' + pyDir+'\n')
        of.write('Using SciSpark implementation results at ' + ssDir+'\n')
        of.write('Results will be stored at ' + workingDir + '/output.log' + '\n')
        print '-' *80
        of.write('-' *80)

        # # get the implementations data
        # if not _get_python_implementation():
        #     sys.exit(2)

        # # run SciSpark
        # if not _run_scispark_implementation():
        #     sys.exit(2)

        # --- Acquire the data from the different implementations for the tests ---
        allTimesInts, ssNodes, pyNodes, ssEdgeList, pySubgraphs = _get_data(sTime, eTime, pyDir, ssDir)

        # check times between implementations
        test_1(pyNodes, ssDir, allTimesInts)
        print '-' *80
        of.write('-' *80)

        # check number of CEs at each frame
        test_2(allTimesInts, pyNodes, ssNodes)
        print '-' *80
        of.write('-' *80)

        # content in the CEs at each frame
        allCEs = test_3(pyDir, ssDir, pyNodes, allTimesInts)
        print '-' *80
        of.write('-' *80)

        # write mappings of cloudelements in either implementation to a file
        _write_CE_mappings(allCEs)
        
        # check graph objects using edgelists
        test_4(pySubgraphs, ssEdgeList)
        print '-' *80
        of.write('-' *80)

        # open subgraphs
        ssSubgraphs = _get_ssSubgraphs(ssDir)

        # check number of graphs
        test_5(pySubgraphs, ssSubgraphs)
        print '-' *80
        of.write('-' *80)

        # check subgraphs generated between either implementation
        test_6(workingDir, pySubgraphs, ssSubgraphs)
        print '-' *80
        of.write('-' *80)

        # check MCCs generated between either implementation
        test_7(workingDir, pyDir, ssDir)
        print '-' *80
        of.write('-' *80)

if __name__ == '__main__':
    main(sys.argv[1:])

