#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  search_embedding_store.py
#  
import sys
import os
import hnswlib
import json
import fileinput
import locale
import faiss
import argparse
import pickle
#import logging
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from sentence_transformers import SentenceTransformer

if sys.version_info[0] > 2:
   xrange = range

parser = argparse.ArgumentParser()

parser.add_argument( '-faiss', action='store_const', const=True, default=False, dest='loadFaiss', help='load faiss index default: false' )
parser.add_argument( '-search', action='store_const', const=True, default=False, dest='runSearch', help='run interactive search' )
parser.add_argument( '-server', action='store_const', const=True, default=False, dest='webSearch', help='run web search server' )
parser.add_argument( '-index', dest='idxFile', help='vector storage file', required=True, metavar="FILE" )
parser.add_argument( '-records', dest='recsFile', help='records file', required=True, metavar="FILE" )
parser.add_argument( '-prefixes', dest='pfxFile', help='prefixes file', required=True, metavar="FILE" )
parser.add_argument( '-port', dest='port', type=str, default="8888", help='server port', metavar="SYMBOL" )
parser.add_argument( '-query', dest='query', type=str, help='query', metavar="SYMBOL" )

locale.setlocale( locale.LC_ALL, '')

DISTANCE = 20
MAT = 0.35    # maximum acceptable distance threashold

class HttpServerWrapper:
    def __init__(self, prefixes, records, searcher, port):
        def handler(*args):
            RequestHandler(prefixes, records, searcher, *args)
        self._server = ThreadingHTTPServer(('', port), handler)
    def serve_forever(self):
        self._server.serve_forever()
    def server_close(self):
        self._server.server_close()
            
class RequestHandler(BaseHTTPRequestHandler):
    _startTime = datetime.now()
    
    def __init__(self, prefixes, records, searcher, *args):
        self._searcher = searcher
        self._prefixes = prefixes
        self._records = records
        BaseHTTPRequestHandler.__init__(self, *args)
    
    def _getResponseTemplate(self):
        return {'data':[], 'message':'', 'count':'0', 'status':'ok'}
        
    def _prepareResponse(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json; charset=utf-8')
        self.end_headers()
    
    def do_GET(self):
        print("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self._prepareResponse()
        d = self._getResponseTemplate()
        d['message'] = f'hearbeat, uptime: {datetime.now() - self._startTime}'
        self.wfile.write(bytes(json.dumps(d), "utf-8"))
        
    def do_POST(self):
        cl = int(self.headers['Content-Length'])
        pd = self.rfile.read(cl).decode('utf-8')
        print(f'CONTENT LENGTH: {cl}\nBODY: {pd}\nHEADERS: {str(self.headers)}')
        self._prepareResponse()
        d = self._getResponseTemplate()
        d['message'] = 'search request'
        
        try:
            j = json.loads(pd)
        except:
            d['status'] = 'failed'
            d['message'] = 'invalid query or invalid json syntaxis'
            self.send_response(400)
            self.wfile.write(bytes(json.dumps(d), "utf-8"))
            return
            
        results = runQuery(j, self._searcher, self._prefixes)
        lst = []
        [lst.append(self._records[i]) for i in results]
        if len(lst) and isinstance(lst[0], dict):
            lst = sorted(lst, key=lambda x: x['name'])
        d['count'] = len(lst)
        [d['data'].append(lst[i]) for i in range(len(lst))]
        #[print(f'{i+1} -> {lst[i]}') for i in range(len(lst))] 
  
        self.wfile.write(bytes(json.dumps(d), "utf-8"))
        
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
        return f'{__class__.__name__}.{self.__op}'
    
    @property
    def results(self):
        return self.__myset    
        
def runQuery(q, searcher, prefixes):
    
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
                rec = filterRecordByDistance(D[0], I[0], prefixes)
                # print(f'? count {len(I[0])} {qq} {D} {I} {set(rec)}')
                # print(f'? count {len(I[0])} {qq} {D} {I}')
                if isinstance(rec, list):
                    op(set(rec))
                cnt += 1
            elif isinstance(qq, dict):
                op(runQuery(qq, searcher, prefixes))
    # print(f'OP: {op.results}') 
    return op.results

#==============================================================================
def filterRecordByDistance(distances, offsets, prefixes):
    lst = []
    for idx in range(len(distances)):
       if distances[idx] <= MAT:
           print(f'PFX: {prefixes[offsets[idx]][0]}')
           lst.extend(prefixes[offsets[idx]][1])
    return lst
    
#==============================================================================
def searchIdx(prefixes, recs, searcher):
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

        results = runQuery(json.loads(q), searcher, prefixes)
        lst = []
        [lst.append(recs[i]) for i in results]
        if len(lst) and isinstance(lst[0], dict):
            lst = sorted(lst, key=lambda x: x['name'])
        print(f'TOTAL: {len(lst)}\nRESULTS: ')
        
        [print(f'{i+1} -> {lst[i]}') for i in range(len(lst))]
        print( "Enter your query below or 'q' to quit: ")
  
#==============================================================================
def fetchRecords(recs, recnums):
      lst = []
      for i in range(len(recnums)):
          lst.append(recs[i])
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
    pfxs = loadRecords(opts.pfxFile)
    # printRecs(recs, 20)
    searcher = SearchWrapper(index, SentenceTransformer('all-MiniLM-L6-v2'))
    # runQuery(json.loads(query), searcher, recs)
    
    print(f'OPTS: {opts} SEARCH: {opts.runSearch} SVR: {opts.webSearch} QUERY: {opts.query}')
    if opts.runSearch:
        searchIdx(pfxs, recs, searcher)
    elif opts.webSearch:
        print(f'Starting server on port {opts.port}\n')
        server = HttpServerWrapper(pfxs, recs, searcher, int(opts.port))

        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        print('Stopping server...\n')
        server.server_close()
    elif not opts.query:
        print("query is empty!\nspecify at least one option: -server , -search or -query")
    
    return 0

#==============================================================================
if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))
    

'''
{"and":["name:mustafa"]}
{"or":["name:mostafa"]}
{"and":["name:mustafa", "aliases:muhammed"]}
{"and":["name:mustafa", {"or":["aliases:muhammed", "aliases:abu"]}]}
{"and": ["type:p", {"or": [{"and":["name:mustafa", {"or":["aliases:muhammed", "aliases:abu"]}]},"name:mostafa"]}]}
{"or": [{"and":["name:mustafa", {"or":["aliases:muhammed", "aliases:abu"]}]},"name:mostafa"]}
{"and":["name:mustafa", "aliases:muhamm"]}
'''
