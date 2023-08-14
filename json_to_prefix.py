#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  json_to_prefix.py
#

import sys
import json
import fileinput
import locale
import argparse

if sys.version_info[0] > 2:
   xrange = range

parser = argparse.ArgumentParser()
parser.add_argument( '-out', dest='pfxFile', help='prefixes file', required=True, metavar="FILE" )
parser.add_argument( '-input', dest='jsonFile', help='prefixes file', required=True, metavar="FILE" )

locale.setlocale( locale.LC_ALL, '')

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
def updatePfxMap(map, key, id):
    if key in map:
        map[key].append(id)
    else:
        map[key] = [id]
    return map
#==============================================================================
def jsonToPfxLst(pfx, pfxmap, id, lst):
    print(lst)

    for i in lst:
        if isinstance(i, dict):
            jsonToPfx(f'{pfx}:', pfxmap, id, i)
        elif isinstance(i, list):
            jsonToPfxLst(f'{pfx}:', pfxmap, id, i)
        else:
            key = f'{pfx}{i}'
            updatePfxMap(pfxmap, key, id)

#==============================================================================
def jsonToPfx(pfx, pfxmap, id, doc):
    print(doc)

    for k in doc:
        print(f'--> {k}')
        if isinstance(doc[k], dict):
            jsonToPfx(f'{pfx}{k}:', pfxmap, id, doc[k])
        elif isinstance(doc[k], list):
            jsonToPfxLst(f'{pfx}{k}:', pfxmap, id, doc[k])
        else:
            key = f'{pfx}{k}:{doc[k]}'
            updatePfxMap(pfxmap, key, id)

#==============================================================================
def main(args):
    print(args)
    # opts = parser.parse_args()
    pfxmap = {}
    print(testjs)
    jsonToPfx("", pfxmap, 0, json.loads(testjs))

    print(json.dumps(pfxmap, indent=1))

    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))



