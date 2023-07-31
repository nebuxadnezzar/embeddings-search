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
import re
import sqlite3
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

not_rx = re.compile('^!(.*)', re.S)
wc_rx_a = re.compile('[*]', re.S)
wc_rx_q = re.compile('[?]', re.S)
wc_rx = re.compile('(?s).*[*?].*')

SET_OPERANDS = set(['and', 'or'])
DISTANCE = 10 #20 probing distance
MAT = 0.089    # maximum acceptable distance threashold
WEL = 20      # wild card expansion limit
NEL = 200     # not expansion limit

class HttpServerWrapper:
    def __init__(self, prefixes, records, searcher, dbconn, port):
        def handler(*args):
            RequestHandler(prefixes, records, searcher, dbconn, *args)
        self._server = ThreadingHTTPServer(('', port), handler)
    def serve_forever(self):
        self._server.serve_forever()
    def server_close(self):
        self._server.server_close()

class RequestHandler(BaseHTTPRequestHandler):
    _startTime = datetime.now()

    def __init__(self, prefixes, records, searcher, dbconn, *args):
        self._searcher = searcher
        self._prefixes = prefixes
        self._records = records
        self._dbconn  = dbconn
        BaseHTTPRequestHandler.__init__(self, *args)

    def _getResponseTemplate(self):
        return {'data':[], 'message':'', 'count':'0', 'status':'ok'}

    def _prepareResponse(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json; charset=utf-8')
        self.end_headers()

    def do_GET(self):
        print("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self._prepareResponse(200)
        d = self._getResponseTemplate()
        d['message'] = f'hearbeat, uptime: {datetime.now() - self._startTime}'
        self.wfile.write(bytes(json.dumps(d), "utf-8"))

    def do_POST(self):
        cl = int(self.headers['Content-Length'])
        pd = self.rfile.read(cl).decode('utf-8')
        print(f'CONTENT LENGTH: {cl}\nBODY: {pd}\nHEADERS: {str(self.headers)}')
        d = self._getResponseTemplate()
        d['message'] = 'search request'

        try:
            j = json.loads(pd)
        except:
            d['status'] = 'failed'
            d['message'] = 'invalid query or invalid json syntaxis'
            self._prepareResponse(400)
            self.wfile.write(bytes(json.dumps(d), "utf-8"))
            return

        results = runQuery(j, self._searcher, self._prefixes, self._dbconn)
        '''
        lst = []
        for i in results:
            lst.append(self._records[i])
        '''
        lst = fetchRecords(j, results, self._records)
        #if len(lst) and isinstance(lst[0], dict):
        #    lst = sorted(lst, key=lambda x: x['name'])
        d['count'] = len(lst)
        d['data'] = lst
        #for i in range(len(lst))
        #    d['data'].append(lst[i])
        self._prepareResponse(200)
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
        indexer.set_ef(50)
        indexer.set_num_threads(4)

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

#==============================================================================
def fetchRecords(query_json, results, recs):
    lst = []
    filter_fields = query_json['filter_fields'] \
                   if 'filter_fields' in query_json and \
                   isinstance(query_json['filter_fields'], list) else []
    if not results:
        return lst
    for i in results:
        rec1 = recs[i]
        rec2 = {}
        for f in filter_fields:
            rec2[f] = rec1[f] if f in rec1 else f'unknown field{f}'
        lst.append(rec2 if rec2 else rec1)

    return lst
#==============================================================================
def isWildCardPresent(term):
        return re.match(wc_rx, term)
#==============================================================================
def isNotConditionPresent(term):
    return re.match(not_rx, term)
#==============================================================================
def pfxsToDb(pfxs):
    dbname = ':memory:' #'pfxs.db'
    sql = 'create table if not exists pfx(id integer primary key autoincrement , prefix text);'
    sqlite3.threadsafety = 3
    conn = sqlite3.connect(dbname, check_same_thread = False)
    conn.execute(sql)
    # conn.execute('delete from pfx')
    for pfx in pfxs:
        sql = f"insert into pfx(prefix) values('{pfx[0]}')"
        conn.execute(sql)

    conn.commit()
    return conn
#==============================================================================
def wildCardToQueryObj(term, dbconn):
    t = re.sub(wc_rx_q, '_', re.sub(wc_rx_a, '%', term))
    #print(t)

    recs = dbconn.execute(f""" select distinct prefix from pfx where prefix like '{t}'\
                               order by length(prefix) limit {WEL}""")
    lst = []
    for rec in recs:
        lst.append(rec[0])
    return {"or":lst}, len(lst) > 0

#==============================================================================
def notToQueryObj(term, dbconn):
    t = re.match(not_rx, re.sub(wc_rx_q, '_', re.sub(wc_rx_a, '%', term))).group(1)
    ss = t.split(':')
    prefix = f" like '{':'.join(ss[:len(ss) - 1])}%' and prefix " if len(ss) > 1 else ''
    sql = f"""select prefix from pfx where prefix {prefix}
              not like '{t}' order by prefix limit {NEL}"""

    recs = dbconn.execute(sql)
    lst = []
    for rec in recs:
        lst.append(rec[0])
    return {"or":lst}, len(lst) > 0

#==============================================================================
def runQuery(q, searcher, prefixes, dbconn):

    # do query validation somewhere here
    # each query is map of list and each list may contain
    # strings to search or other maps of list
    # each map can have only "and" or "or" as keys
    # so below we expect to see only single key - "and" or "or"
    cnt = 1
    for k in q:
        if k not in SET_OPERANDS:
            continue
        op = setOp(k)
        print(f'{k} -> {q[k]} {op}')
        for qq in q[k]:
            if isinstance(qq, str):
                sys.stdout.write(f'{cnt}\n')

                if isNotConditionPresent(qq):
                    obj, ok = notToQueryObj(qq, dbconn)
                    if ok:
                        op(runQuery(obj, searcher, prefixes, dbconn))
                elif isWildCardPresent(qq):
                    obj, ok = wildCardToQueryObj(qq, dbconn)
                    if ok:
                        op(runQuery(obj, searcher, prefixes, dbconn))
                else:
                    D, I = searcher.search(qq)
                    rec = filterRecordByDistance(D[0], I[0], prefixes)
                    if isinstance(rec, list):
                        op(set(rec))
                '''
                if not isWildCardPresent(qq):
                    D, I = searcher.search(qq)
                    rec = filterRecordByDistance(D[0], I[0], prefixes)
                    if isinstance(rec, list):
                        op(set(rec))
                else:
                    obj, ok = wildCardToQueryObj(qq, dbconn)
                    if ok:
                        op(runQuery(obj, searcher, prefixes, dbconn))
                '''
                cnt += 1
            elif isinstance(qq, dict):
                op(runQuery(qq, searcher, prefixes, dbconn))
    # print(f'OP: {op.results}')
    return op.results

#==============================================================================
def filterRecordByDistance(distances, offsets, prefixes):
    lst = []
    for idx in range(len(distances)):
       if distances[idx] <= MAT:
           print(f'PFX: {prefixes[offsets[idx]][0]}\t{distances[idx]}')
           lst.extend(prefixes[offsets[idx]][1])
    return lst

#==============================================================================
def searchIdx(prefixes, recs, searcher, dbconn):
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

        results = runQuery(json.loads(q), searcher, prefixes, dbconn)
        lst = []
        [lst.append(recs[i]) for i in results]
        if len(lst) and isinstance(lst[0], dict):
            lst = sorted(lst, key=lambda x: x['name'])
        print(f'TOTAL: {len(lst)}\nRESULTS: ')

        [print(f'{i+1} -> {lst[i]}') for i in range(len(lst))]
        print( "Enter your query below or 'q' to quit: ")

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
    dbconn = pfxsToDb(pfxs)
    # printRecs(recs, 20)
    searcher = SearchWrapper(index, SentenceTransformer('all-MiniLM-L6-v2'))
    # runQuery(json.loads(query), searcher, recs)

    print(f'OPTS: {opts} SEARCH: {opts.runSearch} SVR: {opts.webSearch} QUERY: {opts.query}')
    try:

        if opts.runSearch:
            searchIdx(pfxs, recs, searcher, dbconn)
        elif opts.webSearch:
            print(f'Starting server on port {opts.port}\n')
            server = HttpServerWrapper(pfxs, recs, searcher, dbconn, int(opts.port))

            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            print('Stopping server...\n')
            server.server_close()
        elif opts.query:
            pass
        else:
            print("query is empty!\nspecify at least one option: -server , -search or -query")
    finally:
        dbconn.close()

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
{"and": [{"or":["name:mustafa","name:mostafa"]}, {"or":["aliases:muhammed", "aliases:abu"]}]}
{"and": [{"or":["name:m?sta*a"]}, {"or":["aliases:muhammed", "aliases:abu"]}]}
{"and": [{"or":["name:m?sta*a"]}]}
{"and": [{"or":["aliases:b*h*h*"]}]}
{"and": [{"or":["aliases:b*h*h?"]}]}
{"and": [{"or":["aliases:b*h*h?"]}, "type:p"]}
{"and": ["type:v",{"or":["name:be*"]} ]}
{"and": ["type:a",{"or":["name:*ark*"]} ]}
{"and": ["type:p",{"or":["name:mark?"]}],"filter_fields":["name", "addresses","aliases"]}
{"and": ["type:p","name:mark?"],"filter_fields":["name", "addresses","aliases"]}
{"and": ["type:v", "nationalities:iran" ], "filter_fields":["name","nationalities"]}
{"and": [{"or":["datesOfBirth:1947"]}]}
'''