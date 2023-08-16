#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  search_prefix_db.py
#
import re
import sys
import json
import fileinput
import locale
import argparse
import sqlite3
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

if sys.version_info[0] > 2:
   xrange = range

parser = argparse.ArgumentParser()
parser.add_argument( '-records', dest='recsFile', help='records file', required=True, metavar="FILE" )
parser.add_argument( '-prefixes', dest='pfxFile', help='prefixes file', required=True, metavar="FILE" )
parser.add_argument( '-port', dest='port', type=str, default="5555", help='server port', metavar="SYMBOL" )
parser.add_argument( '-server', action='store_const', const=True, default=False, dest='webSearch', help='run web search server' )

locale.setlocale( locale.LC_ALL, '')
wc_rx_a = re.compile('[*]', re.S)
wc_rx_q = re.compile('[?]', re.S)
wc_rx = re.compile('(?s).*[*?].*')

SET_OPERANDS = set(['and', 'or'])
WEL = 20      # wild card expansion limit

class HttpServerWrapper:
    def __init__(self, prefixes, records, dbconn, port):
        def handler(*args):
            RequestHandler(prefixes, records, dbconn, *args)
        self._server = ThreadingHTTPServer(('', port), handler)
    def serve_forever(self):
        self._server.serve_forever()
    def server_close(self):
        self._server.server_close()

class RequestHandler(BaseHTTPRequestHandler):
    _startTime = datetime.now()

    def __init__(self, prefixes, records, dbconn, *args):
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
        print(f"GET request,\nPath: {str(self.path)}\nHeaders:\n{str(self.headers)}\n")
        self._prepareResponse(200)
        d = self._getResponseTemplate()
        d['message'] = f'hearbeat, uptime: {datetime.now() - self._startTime}'
        if self.path == '/keys':
            print(f'KEYS: {self._prefixes[0]}\n')
            d['count'] = 1
            d['data'] = self._prefixes[0]
            d['message'] = 'keys request'
        self.wfile.write(bytes(json.dumps(d), "utf-8"))

    def do_POST(self):
        cl = int(self.headers['Content-Length'])
        pd = self.rfile.read(cl).decode('utf-8') # read request body
        print(f'CONTENT LENGTH: {cl}\nBODY: {pd}\nHEADERS: {str(self.headers)}')
        d = self._getResponseTemplate()
        d['message'] = 'search request'

        try:
            j = json.loads(pd)
        except Exception as e:
            print(f'{e}')
            d['status'] = 'failed'
            d['message'] = 'invalid query or invalid json syntaxis'
            self._prepareResponse(400)
            self.wfile.write(bytes(json.dumps(d), "utf-8"))
            return
        wel = getExpansionLimit(j)
        results = runQuery(j, wel, self._prefixes, self._dbconn)

        lst = fetchRecords(j, results, self._records)

        d['count'] = len(lst)
        d['data'] = lst
        self._prepareResponse(200)
        self.wfile.write(bytes(json.dumps(d), "utf-8"))

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
    filter_fields = query_json['select'] \
                   if 'select' in query_json and \
                   isinstance(query_json['select'], list) else []
    if not results:
        return lst
    for i in results:
        rec1 = recs[i]
        rec2 = {}
        for f in filter_fields:
            rec2[f] = rec1[f] if f in rec1 else f'unknown field {f}'
        lst.append(rec2 if rec2 else rec1)

    return lst
#==============================================================================
def isWildCardPresent(term):
        return re.match(wc_rx, term)
#==============================================================================
def pfxsToDb(pfxs):
    dbname = ':memory:' # 'pfxs.db'
    sql = 'create table if not exists pfx(id integer primary key autoincrement, prefix text);'
    sqlite3.threadsafety = 3
    conn = sqlite3.connect(dbname, check_same_thread = False)
    conn.execute(sql)
    conn.execute('create index if not exists prefix_idx on pfx(prefix);')
    for pfx in pfxs:
        if pfx[0] == '_keys_': continue
        sql = f"insert into pfx(prefix) values('{pfx[0]}')"
        conn.execute(sql)

    conn.commit()
    return conn

#==============================================================================
def getExpansionLimit(query_json):
    return  int(query_json['wel']) \
            if 'wel' in query_json and \
            isinstance(query_json['wel'], int) else WEL
#==============================================================================
def prefixSearch(term, wel, dbconn):
    t = re.sub(wc_rx_q, '_', re.sub(wc_rx_a, '%', term))
    # print(f'CHANGED? {t in term} [{t}] [{term}]')
    sql1 = f"= '{t}'"
    sql2 = f"like '{t}' order by length(prefix) limit {wel}"
    sql = ' '.join([f"select id from pfx where prefix", sql1 if t in term else sql2] )

    print(f'SQL QUERY: {sql}')
    recs = dbconn.execute(sql)
    lst = []
    for rec in recs:
        lst.append(rec[0])
    return lst, len(lst) > 0
#==============================================================================
def runQuery(q, wel, prefixes, dbconn):
    print(f'EXPANSION LIMIT: {wel}')
    cnt = 1
    for k in q:
        if k not in SET_OPERANDS:
            continue
        op = setOp(k)
        print(f'{k} -> {q[k]} {op}')
        for qq in q[k]:
            if isinstance(qq, str):
                sys.stdout.write(f'{cnt}\n')
                lst = []
                recs, ok = prefixSearch(qq, wel, dbconn)
                for rec in recs:
                    lst.extend(prefixes[rec][1])
                    print(f'---> {rec}')
                op(set(lst))

                cnt += 1
            elif isinstance(qq, dict):
                op(runQuery(qq, wel, prefixes, dbconn))
    # print(f'OP: {op.results}')
    return op.results

#==============================================================================
def searchPfx(prefixes, recs, dbconn):
    print( "Enter your query below or 'q' to quit:")

    for line in sys.stdin:
        if 'q' == line.rstrip():
            break
        if not line:
            continue
        q = line.strip()
        print(f'Running {q} ...')
        try:
            query = json.loads(q)
        except:
            print(f'invalid query or invalid json syntaxis')
            continue
        wel = getExpansionLimit(query)
        results = runQuery(query, wel, prefixes, dbconn)
        lst = fetchRecords(query, results, recs)

        for i in lst:
            print(f'-> {i}')
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
def main(args):

    opts = parser.parse_args()
    recs = loadRecords(opts.recsFile)
    pfxs = loadRecords(opts.pfxFile)

    try:
        dbconn = pfxsToDb(pfxs)
        if opts.webSearch:
            print(f'Starting server on port {opts.port}\n')
            server = HttpServerWrapper(pfxs, recs, dbconn, int(opts.port))

            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            print('Stopping server...\n')
            server.server_close()

        else:
            searchPfx(pfxs, recs, dbconn)
    finally:
        dbconn.close()
    return 0

if __name__ == '__main__':
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
{"and": ["type:p", "name:f*w*", {"or":["datesofbirth:194?"]}], "select":["type", "name", "datesOfBirth"]}
{"and": ["type:p", "name:f*", {"or":["datesofbirth:194?"]}], "wel":40, "select":["type", "name", "datesOfBirth"]}
{"and": ["type:p", "name:f*"], "wel":35, "select":["name"]}
'''