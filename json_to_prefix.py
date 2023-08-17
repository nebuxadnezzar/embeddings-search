#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  json_to_prefix.py
#
import re
import sys
import json
import fileinput
import locale
import argparse

if sys.version_info[0] > 2:
   xrange = range

parser = argparse.ArgumentParser()
parser.add_argument( '-out', dest='pfxFile', help='prefixes file', required=True, metavar="FILE" )
parser.add_argument( '-input', dest='jsonFile', help='json file', required=True, metavar="FILE" )
parser.add_argument( '-idfield', dest='id', type=str, default="", help='id field', metavar="SYMBOL" )

locale.setlocale( locale.LC_ALL, '')

ns_rx = re.compile(r'[{]([^{}]+)[}]')
pu_rx = re.compile(r'[^\w\s]')
stop_words_rx = re.compile(r'\s+(and|y|the|a|d|s|ll|re|ve|your|yours)\s+')

testjs = \
'''
{
    "hello":"world",
    "hi":"bye",
    "names": ["john", "mary"],
    "ids": {"type": "passport", "number":"3456", "issue":{"country":["sweden", "poland"]}}
}
'''

#==============================================================================
def tokenize(vals):
    return re.sub('\s+', ' ', re.sub(stop_words_rx, ' ', re.sub(pu_rx, ' ', vals))).strip().split()
    #return re.sub('\s+', ' ',re.sub(pu_rx, ' ', vals)).strip().split()

#==============================================================================
def updatePfxMap(map, key, id):
    if key in map:
        map[key].append(id)
    else:
        map[key] = [id]
    return map
#==============================================================================
def jsonToPfxLst(pfx, pfxmap, id, lst):
    # print(lst)

    for i in lst:
        #print(f'++> {i}')
        if not i: continue
        if isinstance(i, dict):
            jsonToPfx(f'{pfx}', pfxmap, id, i)
        elif isinstance(i, list):
            jsonToPfxLst(f'{pfx}:', pfxmap, id, i)
        else:
            for val in tokenize(i):
                key = f'{pfx}{val.lower()}'
                # print(f'LST KEY: {key}')
                updatePfxMap(pfxmap, key, id)

#==============================================================================
def jsonToPfx(pfx, pfxmap, id, doc):
    # print(doc)

    for k in doc:
        #print(f'--> {k}')
        if isinstance(doc[k], dict):
            jsonToPfx(f'{pfx}{k}:', pfxmap, id, doc[k])
        elif isinstance(doc[k], list):
            pfxmap['_keys_'].add(f'{pfx}{k}')
            jsonToPfxLst(f'{pfx}{k}:', pfxmap, id, doc[k])
        else:
            for val in tokenize(doc[k]):
                key = f'{pfx}{k}:{val.lower()}'
                pfxmap['_keys_'].add(f'{pfx}{k}')
                updatePfxMap(pfxmap, key, id)

#==============================================================================
def pfxToFile(path, records):
    f = open(path, 'w')

    for rec in records:
        # print(f'--> {rec}')
        f.write(json.dumps(rec))
        f.write("\n")
    f.flush()
    f.close()
#==============================================================================
def main(args):
    # print(testjs)
    opts = parser.parse_args()
    pfxmap = {'_keys_':set([])}
    # jsonToPfx("", pfxmap, 0, json.loads(testjs))

    cnt = 0
    for line in fileinput.input(opts.jsonFile):
        o = json.loads(line)
        if not opts.id:
            jsonToPfx("", pfxmap, cnt, o)
        else:
            jsonToPfx("", pfxmap, o[opts.id] if opts.id in o else cnt, o)
        cnt += 1
        sys.stderr.write( f"\rrecord count: {cnt}")
    sys.stderr.write( "\n")
    pfxmap['_keys_'] = list(pfxmap['_keys_'])
    pfxToFile(opts.pfxFile, pfxmap.items())
    #print(json.dumps(pfxmap, indent=1))

    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))


'''
json_to_prefix.py -in data/sdn.json -o ~/ephemeral/sdn_pfx.txt
json_to_prefix.py -in data/sdn.json -o ~/ephemeral/sdn_pfx.txt -id "_id"
json_to_prefix.py -in data/sdn.json -o ~/ephemeral/sdn_pfx.txt -id "missing-id"
'''

