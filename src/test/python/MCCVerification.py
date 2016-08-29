import glob, numpy, sys, getopt
from netCDF4 import Dataset
from itertools import groupby
from collections import Counter
from datetime import date, datetime, timedelta

# REAMDE: how to run these tests. 
# 		1. Run SciSpark GTG with the MERG files on the repo at /path/
#		2. Run the GTG release &.& with the same MERG files at input
#		 
#		
def compare_times(pyNodes, ssNodes, ssDir, allTimesInts):
	'''
	Purpose: To check the times of files in two nodelist to determine if similar
	Inputs: pyNodes - a list of strings (F##CE##) representing the nodes found in the Python implementation. F## is the frame integer
			ssNodes - a list of strings representing the nodes found in the SciSpark implementation. F## is the datetime of the frame in the format YYYYMMDDhh
			pyDir - string representing the path to the directory with the netCDF files from the python implementation
			allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime and endTime
	Assumptions: Files contain a datetime string
	'''
	print 'Checking the times in the files ...'
	ssFiles = glob.glob(ssDir+'/MERGnetcdfCEs/*.nc')
	ssTimes = sorted(set(map(lambda x: int(x.split('/')[-1].split('CE')[0].split('F')[1]), ssFiles))) 
	pyTimes = sorted(set(map(lambda x: allTimesInts[int(x.split('F')[1].split('CE')[0]) -1], pyNodes)))
	
	if pyTimes == ssTimes:
		return True, (pyTimes, ssTimes)
	else:
		return False, (pyTimes, ssTimes)
	
def compare_num_CEs_per_frame(pyDir, ssDir, pyNodes, ssNodes, allTimesInts):
	'''
	Purpose: To compare the number of number of CEs per a frame
	Inputs: pyDir - string representing the path to the directory with the netCDF files from the python implementation
			ssDir - string representing the path to the directory with the netCDF files from the SciSpark implementation
			pyNodes - a list of strings (F##CE##) representing the nodes found in the Python implementation. F## is the frame integer
			ssNodes - a list of strings representing the nodes found in the SciSpark implementation. F## is the datetime of the frame in the format YYYYMMDDhh
			allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime and endTime
	Outputs: CEs - list of tuples showing the number of CEs found at each frame for either implementation
	Assumptions: Files contain a datetime string 
	'''
	print 'Checking the number of CEs at each frame ...'
	CEs = []

	for t in range(len(allTimesInts)):
		pFiles = sorted(filter(lambda x: x.split('F')[1].split('CE')[0] == str(t+1), pyNodes))
		sFiles = sorted(filter(lambda x: x.split('F')[1].split('CE')[0] == str(allTimesInts[t]), ssNodes))

		if len(pFiles) == len(sFiles):
			CEs.append((allTimesInts[t], len(pFiles)))
		elif len(pFiles) > len(sFiles):
			CEs.append((('python: F'+str(t+1),len(pFiles)), ('scispark:'+str(allTimesInts[t]),len(sFiles))))
		elif len(pFiles) < len(sFiles):
			CEs.append((('python: F'+str(t+1),len(pFiles)), ('scispark:'+str(allTimesInts[t]),len(sFiles))))

	if len(CEs) == 0:
		return True, CEs
	else:
		return False, CEs

def compare_content_in_CEs(pyDir, ssDir, pyNodes, ssNodes, allTimesInts):
	'''
	Purpose: Determine the overlap between the CEs for each frame from the two implementations
	Inputs: pyDir - string representing the path to the directory with the netCDF files from the python implementation
			ssDir - string representing the path to the directory with the netCDF files from the SciSpark implementation
			pyNodes - a list of strings (F##CE##) representing the nodes found in the Python implementation. F## is the frame integer
			ssNodes - a list of strings representing the nodes found in the SciSpark implementation. F## is the datetime of the frame in the format YYYYMMDDhh
			allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime and endTime
	Outputs: CEs - a list of list of tuples [frame, (python_CE, scispark_CE, overlap, %overlap if a float, 
							or number of pts not overlapping when one node is a subset of another)]
	Assumptions: Files contain a datetime string
	'''
	
	print 'Comparing the content for the CEs at each frame ...'
	
	CEs = []
	pyTimes = sorted(set(map(lambda x: allTimesInts[int(x.split('F')[1].split('CE')[0])-1], pyNodes)))

	for t in pyTimes:
		pyData = []
		ssData = []
		maxOverlapFrame = []
		alldiff = []
		checkedSSCEs = []

		pyFormat = str(t)[:4]+'-'+str(t)[4:6]+'-'+str(t)[6:8]+'_'+str(t)[8:10]+':00:00'
		
		# get files associated with what is in the nodelist only 
		pC = filter(lambda x:int(x.split('F')[1].split('CE')[0]) == allTimesInts.index(t)+1, pyNodes)
		pyAllCEs = glob.glob(pyDir+'/MERGnetcdfCEs/*'+pyFormat+'*.nc')
		pFiles = filter(lambda x: x.split(':00:00')[1].split('.nc')[0] in pC, pyAllCEs)
		pyCEs = map(lambda y: y.split('/')[-1], pFiles)
		
		sFiles = glob.glob(ssDir+'/MERGnetcdfCEs/*'+str(t)+'*.nc')
		
		for pyFIdx in range(len(pFiles)):
			f = Dataset(pFiles[pyFIdx], 'r')
			pyData.append(numpy.squeeze(f.variables['brightnesstemp'][:, :, :], axis=0))
			f.close()

		if pyData:
			for ssF in sFiles:
				f = Dataset(ssF, 'r')
				fce = f.variables['CE_brightness_temperature'][:,:]
				# fce = f.variables['ch4'][:,:]
				ssData.append(fce)
				f.close()

				# compare dims
				if pyData[pyFIdx].shape != fce.shape:
					print '!!!Error: Data shapes are different. Aborting tests. Breaking at %s' %(pFiles[pyFIdx], ssF)
					sys.exit(1)	

			# check equality of the two files (equivalent % of similar values). Assumes Python implementation as truth
			for i in range(len(pyData)):
				for y in range(len(ssData)):
					# if ssCE is entirely in python CE, mark it as true and note how much larger the python CE is than the ssCE
					if numpy.count_nonzero(pyData[i] - ssData[y]) == (numpy.count_nonzero(pyData[i]) - numpy.count_nonzero(ssData[y])) and numpy.count_nonzero(pyData[i] - ssData[y]) > 0:
						alldiff.append((pFiles[i].split('/')[-1], sFiles[y].split('/')[-1], True, numpy.count_nonzero(pyData[i] - ssData[y])))
					# check if pyCE is in ssCE
					else:
						alldiff.append((pFiles[i].split('/')[-1], sFiles[y].split('/')[-1], numpy.array_equiv(pyData[i],ssData[y]), (pyData[i] == ssData[y]).sum()*1.0/ pyData[i].flatten().size ))			
			
			# first place all True in maxOverlapFrame, then check for highest %
			for pyCE in pyCEs:
				oneCE = [i for i in alldiff if i[0] == pyCE]
				sortOneCE = sorted(oneCE, key=lambda x:x[3], reverse=True)

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

def write_CE_mappings(workingDir, allCEs):
	'''
	Purpose: Indicate the mappings of the pyCEs to the ssCEs
	Inputs: workingDir - directory for writing mapping files
			allCEs - a list of list of tuples [frame, (python_CE, scispark_CE, overlap, %overlap if a float, 
							or number of pts not overlapping when one node is a subset of another)]
	Outputs: writes a file in the output directory called CEmappings.txt with the data
	'''
	ceMap = []
	a = map(lambda y: y[1],allCEs)
	for i in a:
		ceMap.extend(map(lambda x: (x[0], x[1]), i))

	try:
		with open(workingDir+'/CEmappings.txt', 'w') as f:
			for i in ceMap:
				f.write(i[0] +' --> '+i[1]+'\n')

		of.write('\nWrote the CE mappings at '+workingDir+'/CEmappings.txt \n')
	except:
		of.write('\n!! Problem with writing CEmappings to file \n')
		print '!! Problem with writing CEmappings'

def compare_edgelists(pyEdgeList, ssEdgeList):
	'''
	Compare the edgelist generated between the two implementations
	Inputs: pyEdgeList - a list of list of strings representing the connect nodes within each subgraph
			ssEdgeList - a list of tuples of two strings representing an edge between connected nodes with subgraphs
	Outputs:
	'''
	groupededges = {}
	nodes = set()
	nodesArray = []
	subgraphs = [] #{}
	groups = []
	seenNodes = []
	nodeGraphs = {}
	graphNodes = []

	# below can be used to find the subgraphs from the edgelist generated 
	for i in ssEdgeList:
	    nodes.add(i[0])
	    nodes.add(i[1])

	nodesArray = [i for i in nodes]

	for key, group in groupby(ssEdgeList, lambda x: x[0]):
	    groupededges[key] = [k for k in group]

	for currNode in nodesArray:
		# thetime = (currNode.split('CE')[0]).split('F')[1]
		# if not thetime in groups or not groups:
		# 	groups.append(thetime)
		# groupNum = groups.index(thetime)        
		nodeGraphs[currNode] = cloud_cluster(currNode, groupededges)
    
	# for i in subgraphs.items()[0:10]:
	# 	source = nodesArray.index(subgraphs.items()[0][0])
	# 	print i

	# for i in subgraphs, if key not in seenNodes, add the items to the list, else check each list for node and add to accordingly
	for k, v in nodeGraphs.iteritems():
		if k not in seenNodes and k not in list(set(sum(subgraphs, []))):
			print k, len(subgraphs), len(seenNodes), '****NOT_SEEN****' 
			subgraphs.append(list(sum(v, ())))
			# seenNodes.append(k)
			seenNodes.extend(list(sum(v, ())))
			seenNodes = list(set(seenNodes))
		else:
			for c in range(len(subgraphs)):
				if k in subgraphs[c]:
					print k, len(subgraphs), len(subgraphs[c]), c, ' ****INSIDE*****'
					subgraphs[c].extend(list(sum(v, ())))
					subgraphs[c] = list(set(subgraphs[c]))
					# seenNodes.append(k)
					seenNodes.extend(list(sum(v, ())))
					seenNodes = list(set(seenNodes))
					break

		# seenNodes = list(set(seenNodes))
					
	# 	print k, len(subgraphs), seenNodes
	
	# sys.exit()
	# for i in filter(lambda x: len(x) >3 ,subgraphs):
	# 	print i
	# seenNodes = sorted(seenNodes, key=lambda y: y.split('F')[1].split('CE')[0])
	print seenNodes
	# sys.exit()
	print len(subgraphs), len(seenNodes)
	subgraphs = filter(lambda x: len(x) >3 , map(lambda y: list(set(y)) , subgraphs))
	print subgraphs
	print len(subgraphs)
	# print len(filter(lambda x: len(x) >3 ,subgraphs))

	sys.exit()
    # true check is to determine if two list match for the edges. Order the edgeList for each subgraph by frame num then 
    # compare 1. len of each list; 2. the number of CEs at each frame (groupby operation); if 1. & 2. equal return true for subgraphs
    # extensive test would be to use the mapping from the previous test for each CE to ensure not only lengths are correct, but the CEs are
    # equivalent as well
        
def cloud_cluster(nodeName, groupedges):
	'''
	Find the subgraphs
	Inputs: nodeName - a string representing the node
			groupedges - a key:value where values is a list representing all the nodes connected to the key
	'''
	Edgelist = []
	Stack = [nodeName]
	while len(Stack) > 0:
	    z = Stack.pop()

	    Edges = []
	    if groupedges.has_key(z):
	        Edges = groupedges[z]

	    for i in Edges:
	        Edgelist.append(i)
	        Stack.append(i[1])
	return Edgelist


def test_1(pyNodes, ssNodes, ssDir, allTimesInts):
	''' 
	Purpose: execute the first test to check the times within either implementation
	Inputs: pyNodes - a list of strings (F##CE##) representing the nodes found in the Python implementation. F## is the frame integer
			ssNodes - a list of strings representing the nodes found in the SciSpark implementation. F## is the datetime of the frame in the format YYYYMMDDhh
			ssDir - string representing the path to the directory with the netCDF files from the SciSpark implementation
			allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime and endTime
	Outputs: None
	'''
	
	passed, runTimes = compare_times(pyNodes, ssNodes, ssDir, allTimesInts)
	
	if passed:
		print 'Test 1: The times between implementations are similar'
		of.write('\nTest 1: The times between implementations are similar \n')
		if len(allTimesInts) != len(runTimes[0]):
			print '\t* Note: there are no CEs in either implementation at these times: %s' %sorted(list(set(allTimesInts) - (set(allTimesInts)&set(runTimes[0]))))
			of.write('\t* Note: there are no CEs in either implementation at these times: %s \n' %sorted(list(set(allTimesInts) - (set(allTimesInts)&set(runTimes[0])))))
	else:
		print '!!Test 1: The times between implementations are NOT similar. \npyDir times are %s.\nssDir times are %s.' %(runTimes[0], runTimes[1])
		print '\t*Note: there are no CEs in Python implementations at these times: %s' %list(set(allTimesInts) - (set(allTimesInts)&set(runTimes[0])))
		print '\t*Note: there are no CEs in SciSpark implementation at these times: %s' %list(set(allTimesInts) - (set(allTimesInts)&set(runTimes[1])))
		of.write('\n!!Test 1: The times between implementations are NOT similar. \npyDir times are %s.\nssDir times are %s' %(runTimes[0],runTimes[1]))
		of.write('\t*Note: there are no CEs in Python implementations at these times: %s' %list(set(allTimesInts) - (set(allTimesInts)&set(runTimes[0])))+'\n')
		of.write('\t*Note: there are no CEs in SciSpark implementation at these times: S3' %list(set(allTimesInts) - (set(allTimesInts)&set(runTimes[1])))+'\n')


def test_2(pyDir, ssDir, allTimesInts, pyNodes, ssNodes):
	'''
	Purpose: execute the 2nd test to compare number of CEs at each frame
	Inputs: pyDir - string representing the path to the directory with the netCDF files from the python implementation
			ssDir - string representing the path to the directory with the netCDF files from the SciSpark implementation
			allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime and endTime
			pyNodes - a list of strings (F##CE##) representing the nodes found in the Python implementation. F## is the frame integer
			ssNodes - a list of strings representing the nodes found in the SciSpark implementation. F## is the datetime of the frame in the format YYYYMMDDhh 
	Outputs: None
	'''
	pyCEs = 0
	ssCEs = 0

	passed, CEs = compare_num_CEs_per_frame(pyDir, ssDir, pyNodes, ssNodes, allTimesInts)
	
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
		print '!!Test 2: Different number of CEs at each frames. There are %d CEs in the Python implementation and %d CEs in the SciSpark implementation. \
		\nPlease check! \n %s' %(pyCEs, ssCEs, filter(lambda x: 'python' in str(x[0]),CEs))
		of.write('\n!!Test 2: Different number of CEs at each frames. There are %d CEs in the Python implementation and %d CEs in the SciSpark implementation. \
			\nPlease check! \n %s\n' %(pyCEs, ssCEs, filter(lambda x: 'python' in str(x[0]),CEs)))
		of.write('*'*25)
		of.write('\n All CEs at times are %s\n' %str(CEs))

def test_3(pyDir, ssDir, pyNodes, ssNodes, allTimesInts):
	'''
	Purpose: execute the third test to check the times within either implementation
	Inputs: pyDir - string representing the path to the directory with the netCDF files from the python implementation
			ssDir - string representing the path to the directory with the netCDF files from the SciSpark implementation
			pyNodes - a list of strings (F##CE##) representing the nodes found in the Python implementation. F## is the frame integer
			ssNodes - a list of strings representing the nodes found in the SciSpark implementation. F## is the datetime of the frame in the format YYYYMMDDhh
			allTimesInts - list of ints (YYYYMMDDhh) representing dates between startTime and endTime
	Outputs: allCEs - a list of list of tuples [frame, (python_CE, scispark_CE, overlap, %overlap if a float, 
							or number of pts not overlapping when one node is a subset of another)]
	'''
	accounted = []
	accCeMap = []
	
	passed, allCEs = compare_content_in_CEs(pyDir, ssDir, pyNodes, ssNodes, allTimesInts)
	
	if passed:
		print 'Test 3: Content of CEs at each frame are similar.'
		of.write('\nTest 3: Content of CEs at each frame are similar.')
	else:
		print '!!Test 3: Different content of CEs at each frames.' 
		of.write('\n!!Test 3: Different content of CEs at each frames.' )
		ceMap = []
		a = map(lambda y: y[1],allCEs)
		for i in a:
			ceMap.extend(filter(lambda x: x[2] == True, i))
		print '!! %d CEs are accounted for. They are: \n %s' %(len(ceMap), ceMap)
		of.write('\n!! '+str(len(ceMap))+' CEs are accounted for. They are: \n ' +str(ceMap)+'\n')
		of.write('*'*25)
		print ('*'*25)
		# if accounted for CEs > len(pyNodes) then there were multiple nodes in scispark implementation for one node.
		if len(ceMap) > len(pyNodes):
			accounted = map(lambda x: x[0].split(':00:00')[1].split('.nc')[0],ceMap)
			accCeMap = ceMap
		
		ceMap = []
		a = map(lambda y: y[1],allCEs)
		for i in a:
			ceMap.extend(filter(lambda x: x[2] == False, i))
		print '!! %d CEs are unaccounted for. They are: \n %s' %(len(ceMap), ceMap)
		of.write('\n!! '+str(len(ceMap))+' CEs are unaccounted for. They are: %s \n' %str(ceMap))
		of.write('*'*25)
		print ('*'*25)

		if accounted:
			c = Counter(accounted) - Counter(pyNodes)
			print '%s in the Python implementation represented by multiple CEs in SciSpark implementation.' %c.keys()
			of.write('\n%s in the Python implementation represented by multiple CEs in SciSpark implementation. \n' %c.keys())
			for ce in c.keys():
				print '%s has %d CEs in SciSpark implementation. Namely: %s' %(ce, c.get(ce)+1, map(lambda j: j[1], filter(lambda i: ce in i[0], accCeMap)))
				of.write('%s has %d CEs in SciSpark implementation. Namely: %s \n' %(ce, c.get(ce)+1, map(lambda j: j[1], filter(lambda i: ce in i[0], accCeMap))))
	return allCEs


def main(argv):
	'''
	Assumes outputs from either implementation are available. Args to dirs should point to the top level of the dir, assuming there is a 
			/MERGnetcdfCEs/ with CEsNetcdfs
			/textFiles/ with any outputs necessary for either implementation e.g. file of node names
	'''
	
	global of
	
	try:
		opts, args = getopt.getopt(argv,"hd:t:")

		if len(opts) == 1:
			if not '-h' in opts[0][0]:
				print 'Please run: python checkResultsRe.py -h'
				sys.exit(2)
		
		for opt, arg in opts:
			if opt in '-h':
				print 'python checkResultsRe.py -d <pythonDir, ssDir, workingDir> -t <sTime, eTime> \n time format is YYYYMMDDhh'
				sys.exit()
			elif opt in '-d':	
				dirs = [i for i in arg.split(',')]
				pyDir = dirs[0]
				ssDir = dirs[1]
				workingDir = dirs[2]
			elif opt in '-t':
				times = [i for i in arg.split(',')]
				sTime = int(times[0])
				eTime = int(times[1])

	except getopt.GetoptError:
		print 'Using defaults settings '
		pyDir = '/verification/run241K65OverlapNoCross'
		ssDir = '/verification/CEs'
		workingDir = '/verification/workingDir'
		sTime = 2006091100
		eTime = 2006091212
	
	print 'Starting MCC accuracy tests ...'
	print 'Using Python implementations results at %s' %pyDir
	print 'Using SciSpark implementation results at %s' %ssDir
	print 'Results will be stored at %s in %s' %(workingDir, 'output.log')

	# --- Acquire the data from the different implementations for the tests ---
	startTime = datetime(int(str(sTime)[:4]), int(str(sTime)[4:6]), int(str(sTime)[6:8]), int(str(sTime)[8:10]))
	endTime = datetime(int(str(eTime)[:4]), int(str(eTime)[4:6]), int(str(eTime)[6:8]), int(str(eTime)[8:10]))
	a = [aDate for aDate in [startTime+timedelta(hours=i) for i in xrange(((endTime - startTime).days* 24 + (endTime - startTime).seconds/3600)+1)]]
	allTimesInts = map(lambda x: int(x.strftime('%Y%m%d%H')), a)

	with open(ssDir+'/textFiles/MCCNodesLines_150Area.json', 'r') as sF:
		sFs = sF.readlines()
	ssNodes = sorted (map(lambda x: x[:-1], sFs),  key=lambda x:x.split('F')[1].split('CE')[0])

	with open(pyDir+'/textFiles/CEList.txt', 'r') as pF:
		pFs = pF.readline()
	pyNodes = map(lambda y: y.lstrip(), sorted(pFs[1:-1].replace('\'','').split(','), key=lambda x:x.split('F')[1].split('CE')[0]))

	with open(ssDir+'/textFiles/MCCEdges.txt', 'r') as sF:
		sFs = sF.readlines()
	ssEList = map(lambda x: x+'))', sFs[0].split('List(')[1][:-3].split(')), '))
	ssEdgeList = map(lambda x: ('F'+x.split(',')[0].split('((')[1]+'CE'+x.split(',')[1].split(')')[0], 'F'+x.split(',')[2].split('(')[1]+'CE'+x.split(',')[3].split('))')[0]), ssEList)
	
	with open(pyDir+'/textFiles/MCSList.txt', 'r') as pF:
		pFs = pF.readline()
	pyEdgeList = map( lambda x: x[1:].replace('\'','').split(','), pFs[1:-1].split(']'))
	# --- end acquire data --

	with open (workingDir+'/output.log', 'w') as of:
		of.write('Starting MCC accuracy tests ...\n')
		of.write('Using Python implementations results at ' + pyDir+'\n')
		of.write('Using SciSpark implementation results at ' + ssDir+'\n')
		of.write('Results will be stored at ' + workingDir +'/output.log'+'\n')
		print('-'*80)
		of.write('-'*80)

		# # check times between implementations
		# test_1(pyNodes, ssNodes, ssDir, allTimesInts)
		# print('-'*80)
		# of.write('-'*80)
		
		# # check number of CEs at each frame		
		# test_2(pyDir, ssDir, allTimesInts, pyNodes, ssNodes)
		# print('-'*80)
		# of.write('-'*80)
		
		# # content in the CEs at each frame
		# allCEs = test_3(pyDir, ssDir, pyNodes, ssNodes, allTimesInts)				
		# print('-'*80)
		# of.write('-'*80)

		# # write mappings of cloudelements in either implementation to a file
		# write_CE_mappings(workingDir, allCEs)

		# check edgelist
		print ssNodes
		print ('#'*20)
		test4 = compare_edgelists(pyEdgeList, ssEdgeList)


if __name__ == '__main__':
	main(sys.argv[1:]) 

