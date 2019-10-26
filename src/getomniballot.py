#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to fetch json ballot definition dat from omiballot

Note: current code assumes a 2 digit ballot type. An upgrade
to scan for the max ballot type to zero fill would be useful
(or else convert to a 3 digit BT.
"""

import argparse
import json
import os
import os.path
import re
import urllib.request

DESCRIPTION = """\
Fetch election definition data from omniballot sample ballots. Files
are fetched if not already present.

Creates the files:
  * ./lookups.json
  * ./bt/btnn

Requires the county ID and election ID as parameters. To find these,
use a county lookup to to access the "Accessible Sample Ballot" with
link to omniballot. Use the browser developer tools to find the URL of
the json file loaded, e.g.
https://published.omniballot.us/06075/628/styles/lookups.json
with the 2 parameters 06075 and 628.
"""

VERSION='0.0.1'     # Program version

def parse_args():
    """
    Parse sys.argv and return a Namespace object.
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION,
                    formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='enable verbose info printout')
    parser.add_argument('-t', dest='test', action='store_true',
                        help='test mode, print files to fetch')
    parser.add_argument('-r', dest='replace', action='store_true',
                        help='refetch and replace existing files')
    parser.add_argument('stcty', help='2 digit state ID + 3 digit county ID')
    parser.add_argument('electid', help='3 digit election ID')

    args = parser.parse_args()

    return args

# lookups.json contain the precinct-bt mappings and uri for base ballot
LOOKUPS_URL = "https://published.omniballot.us/{}/{}/styles/lookups.json"
LOOKUPS = 'lookups.json'
# the base ballot includes a list of precincts. The GETBT_PREFIX with base uri
# constructs a filtered json, with precinct: (0) and moves boxes: to
# ballot.boxes:
GETBT_PREFIX = 'https://lambda.omniballot.us/StyleToOnlineBallot?data='

def getfile(url, filename):
    if not os.path.isfile(filename):
        print(f'getfile: {filename} <- {url}')
        if not args.test:
            urllib.request.urlretrieve(url, filename)

def getjsonfile(url, filename=None):
    if not filename:
        path, filename = os.path.split(url)
    getfile(url,filename)
    if args.test: return
    with open(filename, encoding='utf-8') as f:
        return json.load(f, )

def put_json_file(j, filename):
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(j, f, skipkeys=True, ensure_ascii=False)



# get the list of styles and precincts in LOOKUPS_URL saved to file lookups.json

'''
Notes on the format of lookups.json
styles[id].code = ballot type
          .json_uri = URL to ballot definition data
          .precinct_ids[] omniballot precinct id list.
   .
   "styles" : {
      "13658_0_478" : {
         "name" : "22",
         "party_id" : 0,
         "id" : 13658,
         "precinct_ids" : [
            35817
         ],
         "json_uri" : "https://s3-us-west-2.amazonaws.com/liveballot4published/06075/463/styles/13658_0_478.json",
         "pdf_uri" : "",
         "code" : "22"
      },

precincts[id].id = omniballot precinct ID (redundant as key and value)
             .pid = official precinct ID
             .pname = (official name or ID ?)
             .sid = (split ID?)
             .sname = (split name or ID?)

    "precincts": {
        "124863": {
            "id": 124863,
            "pid": "1101",
            "pname": "1101",
            "sid": "",
            "sname": ""
        },
pct_to_styles["PID:{{pid}} - SID:{{sid}}"][]=
    list of style.id where the hash pid

    "pct_to_styles": {
        "PID:1101 - SID:": [
            "102351"
        ],

The special precinct ID "composite" and style ID "composite" are used to
represent all precincts and contests.
'''

args = parse_args()
j= getjsonfile(LOOKUPS_URL.format(args.stcty,args.electid))

if args.test: exit()

os.makedirs("bt", exist_ok=True)

for sid, s in j['styles'].items():
    print(f'Fetching Style {sid} for bt {s["code"]}/{s["name"]}')
    m = re.match(r'^(?:Poll BT )?(\d+)$', s["name"])
    if m:
        bt = m.group(1)
    else:
        bt = s["code"]
    bt = bt.zfill(3)
    # See sample above, the styles.[].json_uri has the URL to fetch.
    j = getjsonfile(s['json_uri'], f'bt/bt{bt}.json')
    #
    #put_json_file(j['boxes'], f'bt/btb{s["code"].zfill(3)}.json')
    #j = getjsonfile(GETBT_PREFIX+s['json_uri'],
                    #f'bt/btx{s["code"].zfill(3)}.json')
    #put_json_file(j['ballot']['boxes'], f'bt/btbx{s["code"].zfill(3)}.json')
    #break


