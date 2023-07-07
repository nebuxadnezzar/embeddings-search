#!/usr/bin/python3
 
import sys
import os
import numpy as np
import hnswlib
import json
import fileinput
import locale
import faiss
import argparse
import pickle
from sentence_transformers import SentenceTransformer


if sys.version_info[0] > 2:
   xrange = range

#sys.stderr = sys.stdout

parser = argparse.ArgumentParser()

parser.add_argument( '-faiss', action='store_const', const=True, default=False, dest='createFaiss', help='create faiss index default: true' )
parser.add_argument( '-s', action='store_const', const=True, default=False, dest='runSearch', help='run interactive search' )
parser.add_argument( '-t', dest='idxType', type=str, default="l2", help='index type: l2, ivf, cosine, id', metavar="SYMBOL" )
parser.add_argument( '-i', dest='inFile', help='text file', required=True, metavar="FILE" )
parser.add_argument( '-o', dest='outFile', help='output file', required=True, metavar="FILE" )

locale.setlocale( locale.LC_ALL, '')

IVFTYPE = 'ivf'
PQTYPE  = 'pq'
NHSWTYPES = set(['l2', 'ip', 'cosine'])

def createNhswLibIndex(dim, recs, idxType):
    tp = 'l2'
    if idxType in NHSWTYPES:
        tp = idxType
    sz = len(recs)
    ids = np.arange(sz)  
    index = hnswlib.Index(space=tp, dim=dim)  # possible options are l2, cosine or ip

    # Initializing index - the maximum number of elements should be known beforehand
    index.init_index(max_elements=sz, ef_construction=200, M=16)

    # Element insertion (can be called several times):
    index.add_items(sentence_embeddings, ids)

    # Controlling the recall by setting ef:
    index.set_ef(50)  # ef should always be > k
    
    return index

#==============================================================================
def createFaissIndex(dim, idxType ):
    print(f'\nCREATING FAISS index')
    nlist = dim // 300
    if nlist < 5:
        nlist = 5 
    quantizer = faiss.IndexFlatL2(dim)
    print(f'\t INDEX TYPE: {idxType}\n')
    if IVFTYPE in idxType:
        index = faiss.IndexIVFFlat(quantizer, dim, nlist)
    elif PQTYPE in idxType:
        m = 8  # number of centroid IDs in final compressed vectors
        bits = 8 # number of bits in each centroid
        index = faiss.IndexIVFPQ(quantizer, dim, nlist, m, bits) 
    else:
        index = quantizer
        
    index.nprobe = 20
    return index

#==============================================================================
'''
def isdir(d):
        return os.path.exists( d ) and os.path.isdir(d)
'''

def txtToJson(text):
    ok = True
    j = None
    try:
        j = json.loads(text)
    except:
        ok = False  
    return j, ok
#==============================================================================  
def loadRecords(path):
    recs = []
    for line in fileinput.input( path ):
        l = line.strip('\n')
        j, ok = txtToJson(l)
        if ok:
            recs.append(j)
        else:
            recs.append(l)

    return recs
#==============================================================================
def loadFile(path):
    recs = []
    for line in fileinput.input( path ):
        recs.append(line.strip('\n'))

    return recs
        
#==============================================================================
def searchIdx(recs, model, index, isFaiss):
    print( "Enter your query below:")
    
    for line in sys.stdin:
        if 'q' == line.rstrip():
            break
        if not line:
            continue

        print(f'Running {line.strip()} ...')
        
        k = 4 # distance
        xq = model.encode([line]) # query

        if isFaiss:
            D, I = index.search(xq, k)
            
        else:
            D, I = searchWithHnsw(index, xq, k)
        
        print(f'{D}\n{I}')
        [print(f'{i}: {recs[i]}') for i in I[0]]   
        print( "Enter your query below: ")
  
#==============================================================================
def searchWithHnsw(index, query, dist):
    labels, distances = index.knn_query(query, dist)
    return distances, labels   
#==============================================================================

if __name__ == "__main__":
    opts = parser.parse_args()
    
    recs = loadRecords(opts.inFile)
    cnt = 1
    for rec in recs :
        print( rec )
        cnt += 1
        if cnt > 1000:
            break

    print("CREATING EMBEDDINGS...\n")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    sentence_embeddings = model.encode(recs, normalize_embeddings=True) #.tolist()
    
    shape = sentence_embeddings.shape
    print(sentence_embeddings.shape)
   
    if opts.createFaiss :
        index =  createFaissIndex(shape[1], opts.idxType)
        if not index.is_trained:
            print("Training FAISS...")
            index.train(sentence_embeddings)
        index.add(sentence_embeddings)
        print(f'index total: {index.ntotal}')
        faiss.write_index(index, opts.outFile)
    
    else:
        print('CREATE HNSWLIB')
        index = createNhswLibIndex(shape[1], recs, opts.idxType )
        pickle.dump( index, open( opts.outFile, 'wb') )
        
    if opts.runSearch:
        searchIdx(recs, model, index, opts.createFaiss)
    
    # bee is sitting on yellow flower
    # byciclist sitting on a bike
    # aliases:Ruhollah BAZGHANDI
    #with open(f'./embeddings_X.npy', 'wb') as fp:
    #    np.save(fp, sentence_embeddings[0:256])
    # head -2 sdn.json_idx.json | awk '{gsub(/\"|\[/,"",$1);print $1}' FS=,
    # awk '{gsub(/\"|\[/,"",$1);print $1}' FS=, sdn.json_idx.json > sdn-prefixes.txt
