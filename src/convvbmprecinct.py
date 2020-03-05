#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to convert the vbmprecinct.tsv file to area/group/party multirow
and extract the distpct.tsv

"""

import os
import os.path
import sys
import re
import argparse

from tsvio import TSVReader, TSVWriter
from zipfile import ZipFile
from natsort import natsorted
from gzip import GzipFile

DESCRIPTION = """\
Convert the turnout-raw/vbmprecinct.tsv into:

* vbmparty.tsv - (area,group,party[]) lines where group is:
    issued, returned, accepted, pending, challenged

* sdistpct.tsv - Summary areas with list of precincts

Group codes:
IS = VBM ballots issued
RT = VBM ballots returned
AC = VBM ballots accepted
PN = VBM ballots pending
CH = VBM ballots challenged

Requires the following EMS files:
    "../ems/distpct.tsv.gz" list of precincts by district
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
    parser.add_argument('-p', dest='pipe', action='store_true',
                        help='use pipe separator else tab')

    args = parser.parse_args()

    return args


args = parse_args()
separator = "|" if args.pipe else "\t"

# Load the distpct.tsv file
distpct_header = "Precinct_Set|District_Codes|Precincts"

precinctNeigh = {}
with GzipFile("../ems/distpct.tsv.gz") as rzip:
    with TSVReader(rzip,validate_header=distpct_header,binary_decode=True) as f:
        for (Precinct_Set, District_Codes, Precincts) in f.readlines():
            if not District_Codes.startswith('NEIG'):
                continue
            for pct in Precincts.split():
                precinctNeigh[pct] = District_Codes


distHeadMap=dict(
    Congressional='CONG', Assembly='ASSM',
    Supervisorial='SUPV', Neighborhood='NEIG',
    )

ordermap = dict([(v,str(i)) for i,v in enumerate(distHeadMap.values())])



sdistpct = {}
sdisttotal = {}

partyCodeHeader = "TO|AI|DEM|GRN|LIB|PF|REP|NPP"
partyCodes = partyCodeHeader.split('|')
partyHeadings = "|American_Independent_|Democratic_|Libertarian_|Peace_&_Freedom_|Republican_|No_Party_Preference_".split('|')

groupCodes = "IS|RT|AC|PN|CH".split('|')
groupHeadings = "Issued|Returned|Accepted|Pending|Challenged".split('|')

VBM_header = "VotingPrecinctID|VotingPrecinctName|MailBallotPrecinct|BalType"\
    "|Assembly|Congressional|Senatorial|Supervisorial|Issued"\
    "|American_Independent_Issued|Democratic_Issued|Green_Issued"\
    "|Libertarian_Issued|Peace_&_Freedom_Issued|Republican_Issued"\
    "|No_Party_Preference_Issued|No_Party_Preference_(AI)_Issued"\
    "|No_Party_Preference_(LIB)_Issued|No_Party_Preference_(DEM)_Issued"\
    "|Returned|American_Independent_Returned|Democratic_Returned"\
    "|Green_Returned|Libertarian_Returned|Peace_&_Freedom_Returned"\
    "|Republican_Returned|No_Party_Preference_Returned"\
    "|No_Party_Preference_(AI)_Returned|No_Party_Preference_(LIB)_Returned"\
    "|No_Party_Preference_(DEM)_Returned|Accepted"\
    "|American_Independent_Accepted|Democratic_Accepted|Green_Accepted"\
    "|Libertarian_Accepted|Peace_&_Freedom_Accepted|Republican_Accepted"\
    "|No_Party_Preference_Accepted|No_Party_Preference_(AI)_Accepted"\
    "|No_Party_Preference_(LIB)_Accepted|No_Party_Preference_(DEM)_Accepted"\
    "|Pending|American_Independent_Pending|Democratic_Pending|Green_Pending"\
    "|Libertarian_Pending|Peace_&_Freedom_Pending|Republican_Pending"\
    "|No_Party_Preference_Pending|No_Party_Preference_(AI)_Pending"\
    "|No_Party_Preference_(LIB)_Pending|No_Party_Preference_(DEM)_Pending"\
    "|Challenged|American_Independent_Challenged|Democratic_Challenged"\
    "|Green_Challenged|Libertarian_Challenged|Peace_&_Freedom_Challenged"\
    "|Republican_Challenged|No_Party_Preference_Challenged"\
    "|No_Party_Preference_(AI)_Challenged|No_Party_Preference_(LIB)_Challenged"\
    "|No_Party_Preference_(DEM)_Challenged"

def addGroupCols(area:str):
    if area not in sdisttotal:
        sdisttotal[area]=[[0]*len(partyCodes) for i in range(len(groupCodes))]
    for i,grouptotals in enumerate(sdisttotal[area]):
        for j,v in enumerate(groupcols[i]):
            grouptotals[j] += int(v)


with ZipFile("turnoutdata-raw.zip") as rzip:
    with TSVReader("vbmprecinct.csv",opener=rzip,binary_decode=True,
                    validate_header=VBM_header) as f:
        VBM_turnout = f.loaddict()

    with TSVWriter('vbmparty.tsv',sort=True,sep=separator,
                   header="area_id|subtotal_type|"+partyCodeHeader) as o:
        for r in VBM_turnout.values():
            pct = r['VotingPrecinctID']
            groupcols = []

            for gh,group in zip(groupHeadings,groupCodes):
                cols = [r[ph+gh] for ph in partyHeadings]
                o.addline("PCT"+pct,group,*cols)
                groupcols.append(cols)

            addGroupCols('ALLPCTS')
            for h,c in distHeadMap.items():
                area = precinctNeigh[pct] if c=='NEIG' else c+r[h]
                if area=='CONG13':
                    continue
                if area not in sdistpct:
                    sdistpct[area] = []

                sdistpct[area].append(pct)
                addGroupCols(area)


        # Output district summaries
        def sortkey(k):
            k = k[0]
            order=ordermap.get(k[0:4],' ')
            return order+k[4:] if len(k)>5 else order+'0'+k[4:]


        for area,rows in sorted(sdisttotal.items(), key=sortkey):
            for i,group in enumerate(groupCodes):
                line = o.joinline(area,group,*(sdisttotal[area][i]))
                if area=='CONG13':
                    continue
                if area=='ALLPCTS':
                    o.lines.insert(i,line)
                else:
                    o.lines.append(line)

        o.sort = False

    with TSVWriter('sdistpct.tsv',sort=False,sep=separator,
                   header="area_id|precinct_ids") as o:
        for area,pcts in sorted(sdistpct.items(), key=sortkey):
            o.addline(area,' '.join(pcts))









