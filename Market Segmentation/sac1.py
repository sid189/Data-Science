import sys
import pandas as pd
import numpy as np
from igraph import *
from scipy import spatial

mat1=[]
mat2=[]

def sima(v1, v2, g):
    v11 = g.vs[1].attributes().values() 
    v12 = g.vs[1].attributes().values()
    cosdist = spatial.distance.cosine(v11, v12)  #cosine distance
    simi = 1 - cosdist  # Because cosine similarity = 1 - cosine distance
    return simi

def phase1(g, alp, C):
    V = len(g.vs)
    m = len(g.es)
    c = 0
    flag = 0

    while(flag == 0 and c < 15):
        flag = 1
        for i in range(V):
            mv = -1
            maxdq = 0.0
            clusters = list(set(C))
            for j in clusters:
                if (C[i] != j):
                    dq = deltaq(alp, C, g, i, j)
                    if (dq > maxdq):
                        maxdq = dq
                        mv = j
            if(maxdq > 0.0 and mv != -1):
                flag = 0
                C[i] = mv
        c = c + 1
    return C

def phase2(g, C):
    nclust = seqclusters(C)
    temp = list(Clustering(nclust))
    L = len(set(nclust))
    mat1 = np.zeros((L,L))
    
    for i in range(L):
        for j in range(L):
            sim = 0.0
            for k in temp[i]:
                for l in temp[j]:
                    sim = sim + mat2[k][l]
            mat1[i][j] = sim
    g.contract_vertices(nclust)
    g.simplify(combine_edges=sum)
    return

def qattr(C, g):
    clusters = list(Clustering(C))
    V = g.vcount()
    S = 0.0
    
    for c in clusters:
        n = len(c)
        T = 0.0
        for v1 in c:
            for v2 in C:
                if (v1 != v2):
                    T = T + mat1[v1][v2]
        T = T/n
        S = S + T
    return S/(len(set(C)))

def seqclusters(C):
    map = {}
    nclust = []
    c = 0
    for i in C:
        if i in map:
            nclust.append(map[i])
        else:
            nclust.append(c)
            map[i] = c
            c = c + 1
    return nclust

def compmod(g, C):
    return g.modularity(C, weights='weight') + qattr(C, g)

def deltaq(alp, C, g, v1, v2):
    d1 = deltaqnewman(C, g, v1, v2)
    d2 = deltaqattr(C, g, v1, v2)
    return (alp*d1) + ((1-alp)*d2)

def deltaqnewman(C, g, v1, v2):
    Q1 = g.modularity(C, weights='weight')
    temp = C[v1]
    C[v1] = v2
    Q2 = g.modularity(C, weights='weight')
    C[v1] = temp
    return (Q2-Q1);

def deltaqattr(C, g, v1, v2):
    S = 0.0;
    indices = [i for i, x in enumerate(C) if x == v2]
    for v in indices:
        S = S + mat1[v1][v]
    return S/(len(indices)*len(set(C)))

def writef(clusters, filesuf):
    f = open("communities_"+filesuf+".txt", 'w+')
    for c in clusters:
        for i in range(len(c)-1):
            f.write("%s," % c[i])
        f.write(str(c[-1]))
        f.write('\n')
    f.close()

def main(alp):
    att = pd.read_csv('data/fb_caltech_small_attrlist.csv')
    
    V = len(att)  #Determining no. of vertices
    
    fname = 'data/fb_caltech_small_edgelist.txt'
    
    with open(fname) as file:
        e = file.readlines()
    e = [tuple([int(x) for x in line.strip().split(" ")]) for line in e]  #edges
    
    g = Graph()
    g.add_vertices(V)
    g.add_edges(e)
    g.es['weight'] = [1]*len(e)
    
    global mat1, mat2
    
    for column in att.keys():
        g.vs[column] = att[column]
        
    mat1 = np.zeros((V, V))
    
    for i in range(V):
        for j in range(V):
            mat1[i][j] = sima(i, j, g)
    
    mat2 = np.array(mat1)  #Creating a copy of 1st matrix
    
    V = g.vcount()  # Vertex count
    C = phase1(g, alp, range(V))
    print('No. of communities after phase 1:')
    print(len(set(C)))
          
    C = seqclusters(C)

    mod = compmod(g, C)
          
    phase2(g, C)
          
    V = g.vcount()  # Vertex count
    C2 = phase1(g, alp, range(V))
    Cn2 = seqclusters(C2)
    cp2 = list(Clustering(Cn2))
    mod2 = compmod(g, C)
         
    filesuf = 0
          
    if alp == 0.0:
        filesuf = 0
    elif alp == 0.5:
        filesuf = 5
    elif alp == 1.0:
        filesuf = 1
          
    fincluster=[]
    
    Cn = seqclusters(C)
    cp1 = list(Clustering(Cn))
          
    for c in cp2:
        fin1=[]
        for vert in c:
            fin1.extend(cp1[vert])
        fincluster.append(fin1)
          
    if(mod > mod2):
        writef(cp1, str(filesuf))
        print('Higher modularity: Phase 1')
        return cp1
    else:
        writef(cp2, str(filesuf))
        print('Higher modularity: Phase 2')
        return cp2
          
if __name__ == "__main__":
    main(float(sys.argv[1]))
