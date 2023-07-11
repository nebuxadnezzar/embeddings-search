#!/usr/bin/env python
 
import sys
import os
import re
import json
import uuid
import locale
import argparse
import xml.etree.ElementTree as ET

if sys.version_info[0] > 2:
   xrange = range

#sys.stderr = sys.stdout

parser = argparse.ArgumentParser()

parser.add_argument( '-i', dest='inFile', help='Either sdn.xml OFAC file OR 1-document-per-line sdn.json file', required=True, metavar="FILE" )
parser.add_argument( '-o', dest='outFile', help='output file', required=True, metavar="FILE" )

ns_rx = re.compile(r'[{]([^{}]+)[}]')
pu_rx = re.compile(r'[^\w\s]')
stop_words_rx = re.compile(r'\s+(and|y|the|a|d|s|ll|re|ve|your|yours)\s+')
#stop_words_rx = re.compile(r'\b(and|y|the|a|d|s|ll|re|ve|your|yours)\b')
#json_rx = re.compile(r'"[A-Z0-9]+"\s*:\s*"([^"]+)?"', re.I)

locale.setlocale( locale.LC_ALL, '')

entity_types = {'Entity': 'O', 'Individual': 'P', 'Vessel': 'V', 'Aircraft': 'A'}
id_map = {'idNumber':'id', 'idType':'type', 'idCountry':'country'}
adr_lst = ['address1', 'city', 'postalCode', 'country']

#==============================================================================
def parseXML(xmlfile):
    # create element tree object
    tree = ET.parse(xmlfile)
  
    # get root element
    root = tree.getroot()
    elist = []    
    
    for child in root:
        ns = {'d': re.match(ns_rx, child.tag).group(1)}
        if 'sdnEntry' in child.tag :
            break
    #print("NS ", ns) 
    freq = {} # frequency map

    for item in root.findall("d:sdnEntry", ns):
        e = {'src':'ofac', '_id': str(uuid.uuid4())}
        akalist = [] # alias list
        occlist = [] # occupations list
        idlist = []  # identifications list
        nalist = []  # nationalities list
        cilist = []  # citizenships list
        doblist = [] # DOB list
        adrlist = [] # address list
        remlist = [] # remarks list

        fn = ''
        ln = ''
        for c in item:
            if 'sdnType' in c.tag:
                e['type'] = entity_types[c.text]
                updateFreq('type', e['type'], len(elist), freq)
            elif 'uid' in c.tag:
                e['srcId'] = c.text
                updateFreq('srcId', e['srcId'], len(elist), freq)
            elif 'lastName' in c.tag:
                ln = c.text
            elif 'firstName' in c.tag:
                fn = c.text
            elif 'akaList' in c.tag:
                for a in c:
                   aka = processAkaItem(a)
                   updateFreq('aliases', aka, len(elist), freq)
                   akalist.append(aka)
            elif 'title' in c.tag:
                occ = c.text.strip()
                updateFreq('occupations', occ, len(elist), freq)
                occlist.append(occ)
            elif 'idList' in c.tag:
                for i in c:
                    ii = processIdItem(i)
                    updateFreqWithMap(ii, len(elist), freq, 'identifications')
                    idlist.append(ii)
            elif 'nationalityList' in c.tag:
                for n in c:
                    nat = processNatItem(n)
                    updateFreq('nationalities', nat, len(elist), freq)
                    nalist.append(nat)
            elif 'citizenshipList' in c.tag:
                for z in c:
                    cit = processNatItem(z)
                    updateFreq('citizenships', cit, len(elist), freq)
                    cilist.append(cit)
            elif 'dateOfBirthList' in c.tag:
                for d in c:
                    db = processDobItem(d)
                    updateFreq('dob', db, len(elist), freq)
                    doblist.append(db)
            elif 'addressList' in c.tag:
                for a in c:
                    addr = processAddrItem(a)
                    updateFreqWithMap(addr, len(elist), freq, 'addrs')
                    adrlist.append(addr)
            elif 'vesselInfo' in c.tag:
                nat = processVesselInfo(c)
                updateFreq('nationalities', nat, len(elist), freq)
                nalist.append(nat)
            elif 'remarks' in c.tag:
                remlist.append(c.text)

            #print(c.tag, c.text)
            name = ("%s %s" % (fn, ln)).strip()

        updateFreq('name', name, len(elist), freq)    
        e['name'] = name
        e['aliases'] = akalist
        e['occupations'] = occlist
        e['identifications'] = idlist
        e['nationalities'] = nalist
        e['citizenships'] = cilist
        e['datesOfBirth'] = doblist
        e['addresses'] = adrlist
        e['remarks'] =  remlist
        elist.append(e)
        #if( record_count % 100 == 0 ):
        sys.stderr.write( "\rrecord count: " + str( len(elist) ) )
    sys.stderr.write( "\n")

    # remove dups
    for k in freq:
        freq[k] = list(set(freq[k]))
    return elist, freq

#============================================================================== 
def updateFreqWithMap(mp, recId, freq, prefix=None):
    for item in mp.items():
        key = f'{prefix}:{item[0]}' if prefix else item[0] 
        updateFreq(key, item[1], recId, freq)
    
#============================================================================== 
def updateFreq(key, val, recId, freq):
    if not val:
        return freq
    key = re.sub(ns_rx, '', key)
    val = re.sub('"', '',val).lower()
    
    # vals = re.sub('\s+', ' ', re.sub(pu_rx, ' ', val)).strip().split()
    vals = re.sub('\s+', ' ', re.sub(stop_words_rx, ' ', re.sub(pu_rx, ' ', val))).strip().split()
    #print(vals)
    for v in vals:
        k = f'{key}:{v}'
        if k in freq:
            freq[k].append(recId)
        else:
            freq[k] = [recId]
    '''
    k = f'{key}:{val}'
    if k in freq:
        freq[k].append(recId)
    else:
        freq[k] = [recId]
    '''
    return freq     

#==============================================================================        
def processAkaItem(akaItems):
    fn = ''
    ln = ''
    for a in akaItems:
        if 'firstName' in a.tag:
            fn = a.text
        if 'lastName' in a.tag:
            ln = a.text
    return("%s %s" %(fn, ln)).strip()  

#============================================================================== 
def processIdItem(idItems):
    idobj = {}
    for i in idItems:
        key = re.sub(ns_rx, '', i.tag)
        if key in id_map:
           idobj[id_map[key]] = i.text
    return idobj

#==============================================================================     
def processNatItem(naItems):
    for n in naItems:
        if 'country' in n.tag:
            return n.text
    return ''

#============================================================================== 
def processDobItem(dobItems):
    for d in dobItems:
        if 'dateOfBirth' in d.tag:
            return d.text

#============================================================================== 
def processAddrItem(addrItems):
    obj = {}
    for a in addrItems:
        key = re.sub(ns_rx, '', a.tag)
        if key in adr_lst:
            obj[key] = a.text
        elif 'stateOrProvince' in a.tag:
            obj['province'] = a.text
    return obj
    
#==============================================================================
def processVesselInfo(infoItems):
    for i in infoItems:
        if 'vesselFlag' in i.tag:
            return i.text

#==============================================================================
def isJsonFile(path):
    r = True
    with open(path) as f:
        first_line = f.readline()
    try:
        json.loads(first_line)
    except:
        r = False  
    return r

#==============================================================================
def writeListToFile(records, path):
    f = open(path, 'w')
    
    for rec in records:
        f.write(json.dumps(rec))
        f.write("\n")
    f.flush()
    f.close()

#==============================================================================

if __name__ == "__main__":
    opts = parser.parse_args()
    
    if not isJsonFile(opts.inFile ):
        sdn_list, freq_map = parseXML(opts.inFile)
        #print(json.dumps(freq_map))
        #print(json.dumps(sdn_list))
        writeListToFile(sdn_list, opts.outFile)
        writeListToFile(freq_map.items(), opts.outFile + '_idx.json')