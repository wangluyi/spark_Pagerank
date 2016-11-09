# author: Luyi Wang 
# date: 11.08.2016 
# spark for pagerank 
# mainly refered from: https://apache.googlesource.com/spark/+/477c6481cca94b15c9c8b43e674f220a1cda1dd1/examples/src/main/python/pagerank.py

import re
from operator import add
from pyspark import SparkContext

def computeContribs(neighborNodes, rank):
    """Calculates contributions to the rank of other nodes."""
    num_nodes = len(neighborNodes)
    for url in neighborNodes:
        yield (url, rank / num_nodes)

def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


#initial data, change for the 2 files seperately 
lines = sc.textFile("wasbs:///tmp/soc-LiveJournal.txt", 1)

# Loads all nodes from input file and initialize their neighbors.
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

# Loads all nodes with other node(s) link to from input file and initialize ranks of them to 1.0
ranks = links.map(lambda (url, neighbors): (url, 1.0))

# Calculates and updates node ranks continuously using PageRank algorithm.
iteration = 0	# count how many times for iteration 
convergence = 0    # flag of convergence
while 1:
    # Calculates node contributions to the rank of other nodes.
    # computeContribs returns several (url, received Rank) 
    contribs = links.join(ranks).flatMap(lambda (url, (urls, rank)): computeContribs(urls, rank))

    # Re-calculates node ranks based on neighbor contributions.
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    orderedRanks = ranks.takeOrdered(10, key = lambda x: -x[1]) # this is a list in descending order 
    print orderedRanks

    if iteration == 0:
        rank0 = list(orderedRanks)
    else:
        rank1 = list(orderedRanks)
        for j in xrange(10):
            if(rank1[j][0] != rank0[j][0]):
                rank0 = list(rank1)
                convergence = 0
                break
            if j == 10-1:
                convergence = 1

    iteration += 1
    if convergence == 1:
        break

print "iteration: " + str(iteration)
# Collects all node ranks and print  
for (link, rank) in orderedRanks:
    print "%s has rank: %s." % (link, rank)
sc.stop()
