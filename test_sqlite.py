#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  test_sqlite.py
#  

import sys
import re
import json
import fileinput
import locale
import argparse
import sqlite3

if sys.version_info[0] > 2:
   xrange = range

parser = argparse.ArgumentParser()
parser.add_argument( '-prefixes', dest='pfxFile', help='prefixes file', required=True, metavar="FILE" )
parser.add_argument( '-searchdb', action='store_const', const=True, default=False, dest='runSearch', help='run interactive search' )
parser.add_argument( '-wildcard', action='store_const', const=True, default=False, dest='runWildCard', help='run interactive wildcard conversion' )

locale.setlocale( locale.LC_ALL, '')

wc_rx_a = re.compile('[*]', re.S)
wc_rx_q = re.compile('[?]', re.S)
wc_rx = re.compile('(?s).*[*?].*')
#==============================================================================
def isWildCardPresent(term):
        return re.match(wc_rx, term)
#==============================================================================
def pfxsToDb(pfxs):
    dbname = ':memory:' #'pfxs.db'
    sql = 'create table if not exists pfx(id integer primary key autoincrement , prefix text);'
    sqlite3.threadsafety = 3
    conn = sqlite3.connect(dbname)
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
    print(t)
    
    recs = dbconn.execute(f"select prefix from pfx where prefix like '{t}'")
    lst = []
    for rec in recs:
        lst.append(rec[0])
    return {"or":lst}, len(lst) > 0
   
#==============================================================================
#==============================================================================
def searchDb(dbconn):
    print( "Enter your query below or 'q' to quit:")

    for line in sys.stdin:
        if 'q' == line.rstrip():
            break
        if not line:
            continue
        q = line.strip()
        print(f'Running {q} ...')
        try:
            recs = dbconn.execute(q)
        except:
            print(f'invalid query or invalid json syntaxis')
            continue

        for rec in recs:
            print(f"{rec[0]}\t{rec[1]}")
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
def printRecs(recs, limit):
    cnt = 1
    for rec in recs :
        print( rec )
        cnt += 1
        if cnt > limit:
            break
#==============================================================================  
def main(args):
    print(args)
    opts = parser.parse_args()
    pfxs = loadRecords(opts.pfxFile)
    dbconn = pfxsToDb(pfxs)
    if opts.runSearch:
       searchDb(dbconn) 
    #printRecs(pfxs, 10)
    
    
    for q in ["name:m?sta*a", "addresses:city:n*w y?rk", "name:joe"]:
        if isWildCardPresent(q):
            obj, ok = wildCardToQueryObj(q, dbconn)
            if ok:
               print(obj)
            
    dbconn.close()
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))

'''
select * from pfx where prefix like 'name:m_sta%a'


(
    sqlite3.connect(':memory:')
    .execute("""
        select * 
        from pragma_COMPILE_OPTIONS 
        where compile_options like 'THREADSAFE=%'
    """)
    .fetchall()
)
'''
