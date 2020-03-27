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
from collections import defaultdict
from typing import List, Pattern, Match, Dict, Set, TextIO

DESCRIPTION = """\
Convert the turnout-raw/vbmprecinct.tsv into:

* vbmparty.tsv - (area,group,party[]) lines where group is:
    issued, returned, accepted, pending, challenged

* sdistpct.tsv - Summary areas with list of precincts
* pctsdist.tsv - Precinct groups with list of Summary areas

Group codes:
RG = Total (VBM+ED) registration
IS = VBM ballots issued
RT = VBM ballots returned
AC = VBM ballots accepted
PN = VBM ballots pending
CH = VBM ballots challenged

Requires the following EMS files:
    "../ems/distpct.tsv.gz" list of precincts by district
"""

VERSION='0.0.1'     # Program version

OUT_DIR = "../out-orr/resultdata"
TURNOUT_FILE = f"{OUT_DIR}/turnout.tsv"

if not os.path.exists(OUT_DIR):
    os.makedirs(OUT_DIR)


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
pctsdist = {}
sdisttotal = {}

partyNames = {
    'AI':'American Independent',
    'DEM':'Democratic',
    'GRN':'Green',
    'LIB':'Libertarian',
    'PF':'Peace and Freedom',
    'REP':'Republican',
    'NPP':'No Party Preference',
    }
partyName2ID = {v:k for k,v in partyNames.items()}
partyName2ID['*TOTAL*'] = 'ALL'
partyName2ID['*TOTALS*'] = 'ALL'
partyName2ID['Non-Partisan'] = 'NPP'


partyCodeHeader = "ALL|AI|AINPP|DEM|DEMNPP|GRN|LIB|LIBNPP|PF|REP|NPP"
partyCodes = partyCodeHeader.split('|')
# Add NPPALL as computed total of AINPP+DEMNPP+LIBNPP+NPP
partyCodes2 = partyCodes+['NPPALL']
nppall_i = len(partyCodes)
npp_cols = [i for i,n in enumerate(partyCodes) if n.endswith('NPP')]

print(f"npp_cols={npp_cols}")

partyHeadings = "|American_Independent_|No_Party_Preference_(AI)_|Democratic_|No_Party_Preference_(DEM)_|Green_|Libertarian_|No_Party_Preference_(LIB)_|Peace_&_Freedom_|Republican_|No_Party_Preference_".split('|')

groupCodes = "RSIss|RSCst|RSCnt|RSPnd|RSCha".split('|')
groupHeadings = "Issued|Returned|Accepted|Pending|Challenged".split('|')
groupCodes2 = ['RSRegTO','RSRegED']+groupCodes

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

REGISTRATION_header = "PrecinctName|PrecinctExternalId|ElectorGroupName"\
    "|ElectorGroupExternalId|Count"

def addGroupCols(area:str):
    if area not in sdisttotal:
        sdisttotal[area]=[[0]*len(partyCodes2) for i in range(len(groupCodes2))]
    for i,grouptotals in enumerate(sdisttotal[area]):
        for j,v in enumerate(groupcols[i]):
            grouptotals[j] += int(v)


def sortkey(k):
    k = k[0]
    order=ordermap.get(k[0:4],' ')
    return order+k[4:] if len(k)>5 else order+'0'+k[4:]

Counting_Group2Id = {
    '*TOTALS*':'TO',
    'Election Day':'ED',
    'Vote by Mail':'MV',
    }
Card_Index2Id = {
    '*TOTALS*':'RSVot',
    '1':'RSCd1',
    '2':'RSCd2',
    }


precinctPartyReg = {}
allpctPartyTurnout = defaultdict(int)
with ZipFile("resultdata-raw.zip") as rzip:
    with TSVReader("Registration.txt",opener=rzip,binary_decode=True,
                   validate_header=REGISTRATION_header) as f:
        for (PrecinctName, PrecinctExternalId, ElectorGroupName,
             ElectorGroupExternalId, Count) in f.readlines():
            party_id = partyName2ID[ElectorGroupName]
            precinctPartyReg[f"PCT{PrecinctExternalId}:{party_id}"] = Count

    with TSVReader("BallotGroupTurnout.psv",opener=rzip,binary_decode=True,
                   validate_header="Ballot Group|Counting Group|Card Index|Turnout"
                   ) as f:
        for (Ballot_Group, Counting_Group, Card_Index, Turnout) in f.readlines():
            # party, voting group, card, voters
            if Ballot_Group == '\x0c':
                # Page break
                continue
            Ballot_Group = re.sub(r' By Type$','',Ballot_Group)
            Ballot_Group, n = re.subn(r' NPP$','', Ballot_Group)
            party_id = partyName2ID[Ballot_Group]
            if n:
                party_id += 'NPP'
            voting_group = Counting_Group2Id[Counting_Group]
            card_id = Card_Index2Id[Card_Index]
            allpctPartyTurnout[f'{voting_group}:{card_id}:{party_id}'] +=int(Turnout)

        # Fix missing card totals
        for rs in ['RSCd1','RSCd2']:
            for p in partyCodes:
                allpctPartyTurnout[f'TO:{rs}:{p}']= (
                    allpctPartyTurnout[f'ED:{rs}:{p}']+
                    allpctPartyTurnout[f'MV:{rs}:{p}'])

def NPPCrossover(party_id:str)->str:
    """
    Convert xxxNPP and NPPALL crossover ID to just NPP for registration
    """
    return 'NPP' if (party_id.endswith("NPP")
                    or party_id.startswith("NPP")) else party_id


def getPrecinctPartyReg(area_id:str, party_id:str)->int:
    """
    Retrieve the registration count for an area and party.
    For NPP crossover, use the NPP total
    """
    # Trim party prefix before NPP, so crossover use the whole NPP
    return int(precinctPartyReg.get(f"{area_id}:{NPPCrossover(party_id)}",0))

def appendNPPALL(cols:List[int]):
    """
    Compute the sum of xxxNPP columns and append as NPPALL
    """
    cols.append(sum([int(cols[i]) for i in npp_cols]))


with ZipFile("turnoutdata-raw.zip") as rzip:
    with TSVReader("vbmprecinct.csv",opener=rzip,binary_decode=True,
                    validate_header=VBM_header) as f:
        VBM_turnout = f.loaddict()

    with TSVWriter(TURNOUT_FILE,sort=True,sep=separator,
                   header=f"area_id|subtotal_type|result_stat|{partyCodeHeader}|NPPALL") as o:
        for r in VBM_turnout.values():
            pct = r['VotingPrecinctID']
            pcta = 'PCT'+pct
            groupcols = []

            # Create a pseudo-group for party registration
            cols = [getPrecinctPartyReg(pcta, ph) for ph in partyCodes2]
            o.addline(pcta,'TO','RSReg',*cols)
            groupcols.append(cols)

            # Create computed election-day registration
            cols_mv = [int(r[ph+'Issued']) for ph in partyHeadings]
            appendNPPALL(cols_mv)
            cols_ed = [
                to-mv if to>=mv else 0
                for to,mv in zip(cols, cols_mv)]
            o.addline(pcta,'ED','RSReg',*cols_ed)
            groupcols.append(cols_ed)

            for gh,group in zip(groupHeadings,groupCodes):
                cols = [r[ph+gh] for ph in partyHeadings]
                appendNPPALL(cols)
                o.addline(pcta,'MV',group,*cols)
                groupcols.append(cols)

            addGroupCols('ALLPCTS')
            sdists = []
            # Create reverse map precinct to summary groups
            for h,c in distHeadMap.items():
                area = precinctNeigh[pct] if c=='NEIG' else c+r[h]
                if area=='CONG13':
                    continue
                if area not in sdistpct:
                    sdistpct[area] = []

                sdistpct[area].append(pct)
                sdists.append(area)

                addGroupCols(area)

            # Form a space separated list
            sdist_group = ' '.join(sorted(sdists, key=sortkey))
            if sdist_group not in pctsdist:
                pctsdist[sdist_group] = []
            pctsdist[sdist_group].append(pct)


        # Output district summaries

        for area,rows in sorted(sdisttotal.items(), key=sortkey):
            for i,group in enumerate(groupCodes2):
                m=re.match(r'(RS.*)(TO|ED)$',group)
                if m:
                    group, vg = m.groups()
                else:
                    vg = 'MV'
                line = o.joinline(area,vg,group,*(sdisttotal[area][i]))
                if area=='CONG13':
                    continue
                if area=='ALLPCTS':
                    o.lines.insert(i,line)
                else:
                    o.lines.append(line)

        # Insert turnout for ALLPCTS
        for i, vg in reversed(list(enumerate(Counting_Group2Id.values()))):
            for rs in reversed(list(Card_Index2Id.values())):
                cols = [allpctPartyTurnout[f'{vg}:{rs}:{party_id}']
                        for party_id in partyCodes]
                #if rs.startswith('RSCd'):
                    #cols.append('')
                #else:
                    #appendNPPALL(cols)
                appendNPPALL(cols)
                o.lines.insert(i+1,o.joinline('ALLPCTS',vg,rs,*cols))

        o.sort = False

    with TSVWriter('sdistpct.tsv',sort=False,sep=separator,
                   header="area_id|precinct_ids") as o:
        for area,pcts in sorted(sdistpct.items(), key=sortkey):
            o.addline(area,' '.join(pcts))
    with TSVWriter('pctsdist.tsv',sort=False,sep=separator,
                   header="precinct_ids|area_ids") as o:
        for area,pcts in sorted(pctsdist.items(), key=sortkey):
            o.addline(' '.join(pcts),area)









