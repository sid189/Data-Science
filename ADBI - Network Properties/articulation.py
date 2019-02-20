import sys
import time
import networkx as nx
import pandas
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from copy import deepcopy
from pyspark.sql.types import *

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def articulations(g, usegraphframe=False):
    baseCount=g.connectedComponents().select('component').distinct().count()
    op=[]
    if usegraphframe:
        vertlist=g.vertices.rdd.map(lambda row: row['id']).collect()
        
        for v in vertlist:
            gr=GraphFrame(vertlist.filter('id != "' + v + '"'), g.edges)
            c = gr.connectedComponents().select('component').distinct().count()
            
            op.append((v, 1 if c>baseCount else 0))
        artdf = sqlContext.createDataFrame(sc.parallelize(op), ['id', 'articulation'])
        return artdf
    else:
        # YOUR CODE HERE
        g1=nx.Graph()
        g1.add_nodes_from(g.vertices.map(lambda x:x.id).collect())
        g1.add_edges_from(g.edges.map(lambda x: (x.src, x.dst)).collect())
        
        def comp(n):
            g=deepcopy(g1)
            g.remove_node(n)
            return nx.number_connected_components(g)
        
        artdf1=sqlContext.createDataFrame(g.vertices.map(lambda x: (x.id, 1 if comp(x.id) > baseCount else 0)), ['id', 'articulation'])
        return artdf1

    
filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs,['src','dst'])
e = e.unionAll(e.selectExpr('src as dst','dst as src')).distinct() 

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()

# Create graphframe from the vertices and edges.
g = GraphFrame(v,e)

#Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
print("---------------------------")
df.toPandas().to_csv('articulation.csv')
'''
#Runtime for below is more than 2 hours
print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
init = time.time()
df = articulations(g, True)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
'''