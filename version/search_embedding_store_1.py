#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  search_embedding_store.py
#  
import sys
import os
#import numpy as np
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

parser = argparse.ArgumentParser()

parser.add_argument( '-faiss', action='store_const', const=True, default=False, dest='loadFaiss', help='load faiss index default: false' )
parser.add_argument( '-search', action='store_const', const=True, default=False, dest='runSearch', help='run interactive search' )
parser.add_argument( '-server', action='store_const', const=True, default=False, dest='webSearch', help='run web search server' )
parser.add_argument( '-index', dest='idxFile', help='vector storage file', required=True, metavar="FILE" )
parser.add_argument( '-records', dest='recsFile', help='records file', required=True, metavar="FILE" )
parser.add_argument( '-query', dest='query', type=str, help='query', metavar="SYMBOL" )
# parser.add_argument( '-o', dest='outFile', help='output file', required=True, metavar="FILE" )

locale.setlocale( locale.LC_ALL, '')

DISTANCE = 100
MAT = 0.5    # maximum acceptable distance threashold

query = '''
        {"and": ["address:street:fish", "name:joe", {"or":["alias:shmoe", "alias:joseph"]}, "address:city:brooklyn"]} 
        '''

class FaissIndexWrapper:
    def __init__(self, indexer, distance):
        self.indexer = indexer
        self.distance = distance
        
    def search(self, xq):
        return self.indexer.search(xq, self.distance)
        
class HnswIndexWrapper:
    def __init__(self, indexer, distance):
        self.indexer = indexer
        self.distance = distance
        
    def search(self, xq):
        labels, distances = self.indexer.knn_query(xq, self.distance)
        return distances, labels

class SearchWrapper:
    def __init__(self, indexerWrapper, embModel):
        self.indexerWrapper = indexerWrapper
        self.embModel = embModel
        
    def search(self, q):
        xq = self.embModel.encode([q]) # query
        return self.indexerWrapper.search(xq)

class setOp:
    def __init__(self, op):
        self.__op = op
        self.__myset = None
    
    def setOperation(self, st):
        if self.__myset is not None and st is not None:
            self.__myset = self.__myset.intersection(st) if self.__op == 'and' else self.__myset.union(st)
        else:
            self.__myset = st
        return self.__myset
            
    def __call__(self, st):
        return self.setOperation(st)
    def __str__(self):
        return f'{__class__.__name__}.{self.__op} {type(self.__myset)}'
    
    @property
    def results(self):
        return self.__myset    
        
def runQuery(q, searcher, recs):
    
    # do query validation somewhere here
    # each query is map of list and each list may contain
    # strings to search or other maps of list
    # each map can have only "and" or "or" as keys
    # so below we expect to see only single key - "and" or "or"
    cnt = 1
    for k in q:
        op = setOp(k)
        print(f'{k} -> {q[k]} {op}')
        for qq in q[k]:
            if isinstance(qq, str):
                sys.stdout.write(f'{cnt}')
                D, I = searcher.search(qq)
                rec = filterRecordByDistance(D[0], I[0], recs)
                print(f'? count {len(I[0])} {qq} {D} {I} {rec}')
                if isinstance(rec, list):
                    op(set(rec))
                cnt += 1
            elif isinstance(qq, dict):
                op(runQuery(qq, searcher, recs))
    # print(f'OP: {op.results} {type(op.results)} {op.results.__class_getitem__(int)}') 
    return op.results

#==============================================================================
def filterRecordByDistance(distances, offsets, recs):
    lst = []
    for idx in range(len(distances)):
       if distances[idx] <= MAT:
           lst.extend(recs[offsets[idx]][1])
    return lst
    
#==============================================================================
def searchIdx(recs, searcher):
    print( "Enter your query below or 'q' to quit:")
    
    for line in sys.stdin:
        if 'q' == line.rstrip():
            break
        if not line:
            continue
        q = line.strip()
        print(f'Running {q} ...')
        try:
            j = json.loads(q)
        except:
            print(f'invalid query or invalid json syntaxis')
            continue
        lst = runQuery(json.loads(q), searcher, recs)
        print(f'RESULTS: {lst}')
        '''
        print(f'{D}\n{I}')
        [print(f'{i}: {recs[i]}') for i in I[0]]  
        ''' 
        print( "Enter your query below or 'q' to quit: ")
  
#==============================================================================
  
#==============================================================================
#==============================================================================
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
def loadIdx(path, dist, isFaiss):
    if isFaiss:
        index = FaissIndexWrapper(faiss.read_index(path, faiss.IO_FLAG_MMAP), dist)
    else:
        index = HnswIndexWrapper(pickle.load(open(path, 'rb')), dist)
    return index

#==============================================================================     
def printRecs(recs, limit):
    cnt = 1
    for rec in recs :
        print( rec )
        cnt += 1
        if cnt > limit:
            break
#============================================================================== 
def main(args):
    opts = parser.parse_args()
    index = loadIdx(opts.idxFile, DISTANCE, opts.loadFaiss)
    recs = loadRecords(opts.recsFile)
    # printRecs(recs, 20)
    searcher = SearchWrapper(index, SentenceTransformer('all-MiniLM-L6-v2'))
    # runQuery(json.loads(query), searcher, recs)
    
    print(f'OPTS: {opts} SEARCH: {opts.runSearch} SVR: {opts.webSearch} QUERY: {opts.query}')
    if opts.runSearch:
        searchIdx(recs, searcher)
    elif opts.webSearch:
        # run web svr
        pass
    elif not opts.query:
        print("query is empty!\nspecify at least one option: -server , -search or -query")
        
    # print(query)
    

    return 0

#==============================================================================
if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))
    

'''
{"and":["name:mustafa"]}
{"or":["name:mustafa"]}
{"and":["name:mustafa", "aliases:muhammed"]}
{"and":["name:mustafa", {"or":["aliases:muhammed"]}]}
{"and":["name:mustafa", "aliases:muhamm"]}
'''
