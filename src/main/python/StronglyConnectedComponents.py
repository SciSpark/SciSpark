import json
from itertools import groupby
from ast import literal_eval


''' Purpose: generate graph from edge list
    Inputs: edgelist from function in sciSpark
'''
def main():
    print 'begin!'
    edges = []
    allEdges = []
    groupededges = {}
    vertices = set()
    vertexArray = []
    # Assume edgelist is saved in file as a list of tuples where, [((frameNum, CE),(frameNum, CE))]
    edgelistFile = '/home/rahul/IdeaProjects/SciSpark/EdgeList.txt'
    jsonFile = '/home/rahul/IdeaProjects/SciSpark/graph.json'

    with open(edgelistFile,'r') as f:
        for line in f:
            edges.extend(literal_eval(line.strip()))

    print 'There are %d edges' %(len(edges))

    # sort out the naming and change to [(fromNode, toNode)] to remove edgelist as list of tuples of tuples to a list of tuples
    for eachtuple in edges:
        fromNode = 'F'+str(eachtuple[0][0])+'CE'+str(int(eachtuple[0][1]))
        toNode = 'F'+str(eachtuple[1][0])+'CE'+str(int(eachtuple[1][1]))
        allEdges.append((fromNode,toNode))

    print 'sanity check: length after naming is %d ' %(len(allEdges))
    for i in allEdges:
        vertices.add(i[0])
        vertices.add(i[1])
    for i in vertices:
        vertexArray.append(i)

    for key, group in groupby(allEdges, lambda x: x[0]):
        groupededges[key] = []
        for k in group:
            groupededges[key].append(k)

    print 'Number of vertices %d' %(len(vertexArray))
    solution = {}
    nodesJSON = []
    linksJSON = []

    for x in vertexArray:
        solution[x] = stronglycc(x, groupededges)
        nodesJSON.append({"name":x})

    
    for i in solution.items()[0:10]:
        source = vertexArray.index(solution.items()[0][0])
        print source
        if len(i[1]) < 100:
            for vertexPair in i[1]:
                source = vertexArray.index(vertexPair[0])
                target = vertexArray.index(vertexPair[1])
                linksJSON.append({'source':source, 'target':target})

    with open(jsonFile,'w') as outfile:
        json.dump({"nodes":nodesJSON,"links":linksJSON}, outfile)
            


def stronglycc(x, groupedges):
    Edgelist = []
    Stack = [x]
    while len(Stack) > 0:
        z = Stack.pop()

        Edges = []
        if groupedges.has_key(z):
            Edges = groupedges[z]

        for i in Edges:
            Edgelist.append(i)
            Stack.append(i[1])
    return Edgelist




main()