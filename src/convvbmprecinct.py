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

OUT_DIR = "../out-orr/resultdata"
TURNOUT_FILE = f"{OUT_DIR}/turnout.tsv"

DESCRIPTION = f"""\
Reads:
  * "turnoutdata-raw.zip vbmprecinct.csv" - VBM ballots issued and processed
  * "resultdata-raw.zip Registration.txt" - precinct registration by party
  * "resultdata-raw.zip BallotGroupTurnout.psv" - All-precinct turnout
  * "pctturnout.tsv" - Extracted Voter turnout (and registration) by precinct
  * "turnoutdata-raw.zip regstat.tsv" - with -s
  * "../ems/distpct.tsv.gz" - list of precincts by district

Creates:

  * {TURNOUT_FILE} - (area,group,party[]) lines
      where group is: issued, returned, accepted, pending, challenged
  * sdistpct.tsv - Summary areas with list of precincts
  * pctsdist.tsv - Precinct groups with list of Summary areas
"""

VERSION='0.0.1'     # Program version


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
    parser.add_argument('-x', dest='crossover', action='store_true',
                        help='partisan primary with crossover voting')
    parser.add_argument('-s', dest='regstat', action='store_true',
                        help='use regstat.tsv registration by party summary')
    parser.add_argument('-t', dest='novbmprecinct', action='store_true',
                        help='ignore vbmprecinct.csv, use vbmturnout.tsv')
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

if args.crossover:
    partyCodeHeader = "ALL|AI|AINPP|DEM|DEMNPP|GRN|LIB|LIBNPP|PF|REP|NPP"
    partyCodeHeader2 = partyCodeHeader+'|NPPALL'
    partyCodes = partyCodeHeader.split('|')
    partyCodes2 = partyCodes+['NPPALL']
    partyHeadings = "|American_Independent_|No_Party_Preference_(AI)_|Democratic_|No_Party_Preference_(DEM)_|Green_|Libertarian_|No_Party_Preference_(LIB)_|Peace_&_Freedom_|Republican_|No_Party_Preference_".split('|')

else:
    partyCodes2 = partyCodes = ["ALL"]+list(partyNames.keys())
    partyCodeHeader2 = partyCodeHeader = "|".join(partyCodes)
    partyHeadings = "|American_Independent_|Democratic_|Green_|Libertarian_|Peace_&_Freedom_|Republican_|No_Party_Preference_".split('|')

None_party_cols = [None]*(len(partyCodes2)-1)

# Add NPPALL as computed total of AINPP+DEMNPP+LIBNPP+NPP
nppall_i = len(partyCodes)
npp_cols = [i for i,n in enumerate(partyCodes) if n.endswith('NPP')]

#print(f"npp_cols={npp_cols}")


groupCodes = "RSIss|RSCst|RSCnt|RSPnd|RSCha".split('|')
groupHeadings = "Issued|Returned|Accepted|Pending|Challenged".split('|')
# These are the group codes to summarize in stats
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

PCTTURNOUT_header = "area_id|total_registration|ed_registration|mv_registration"\
    "|total_ballots|ed_ballots|mv_ballots"

# 2018-era pctturnout.tsv header
PCTTURNOUT_header2 = "area_id|registration|total_ballots|ed_ballots|mv_ballots"

REGSTAT_header = "Area|Total Registration|Military and Overseas|Permanent Mail Voters"\
    "|American Independent|Democratic|Green|Libertarian|Peace and Freedom"\
    "|Republican|No Party Preference/Unknown|Chinese|English|Filipino|Hindi"\
    "|Japanese|Khmer|Korean|Russian|Spanish|Thai|Vietnamese|Other"\
    "|Pre-registered Under 18"


def get_zip_filenames(
    zipfile                 # Zip file to read (ZipExtFile)
    )->Set[str]:            # Returned set of filenames
    """
    Reads the directory of filenames in a zip archive and returns the
    list as a set.
    """
    # Create a set with the zip file names
    zipfilenames = set()
    for info in zipfile.infolist():
        zipfilenames.add(info.filename)

    return zipfilenames

def addGroupCols(area:str):
    """
    For a set of stats with area, add the stat values to summary districts in
    sdisttotal[area][stat][party]
    """
    if area not in sdisttotal:
        sdisttotal[area]=[[0]*len(partyCodes2) for i in range(len(groupCodes2))]
    for i,grouptotals in enumerate(sdisttotal[area]):
        for j,v in enumerate(groupcols[i]):
            if v!=None:
                grouptotals[j] += int(v)
def nocomma(s:str):
    """
    convert to int with commas removed
    """
    if isinstance(s, str):
        s = int(s.replace(',',''))
    return s

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

precinctReg = {}        # Precinct Registration from sov extract
precinctBallots = {}    # Ballots returned from sov extract
precinctEDBallots = {}  # ED Ballots cast from sov extract
precinctMVBallots = {}  # ED Ballots cast from sov extract
precinctPartyReg = {}   # Registration by PCTprecint:PartyId from Registration.txt
allpctPartyTurnout = defaultdict(None) #BallotGroupTurnout.psv for all pcts

# Load all party registration and turnout
have_sov_turnout = os.path.exists("pctturnout.tsv")
if have_sov_turnout:
    with TSVReader("pctturnout.tsv") as f:
        if f.headerline==PCTTURNOUT_header:
            format2 = 0
        elif f.headerline==PCTTURNOUT_header2:
            format2 = 1
        else:
            raise RuntimeError(f"Mismatched header in {f.path}:\n   '{f.headerline}'\n!= '{PCTTURNOUT_header}'")

        # We will insert registration
        groupCodes2 = ['RSRegTO','RSVotTO','RSRegED','RSVotED']+groupCodes
        for cols in f.readlines():
            if format2:
                (area_id, total_registration, total_ballots,
                 ed_ballots, mv_ballots) = cols
            else:
                (area_id, total_registration, ed_registration, mv_registration,
                 total_ballots, ed_ballots, mv_ballots) = cols

            precinctReg[area_id] = total_registration
            precinctBallots[area_id] = total_ballots
            precinctEDBallots[area_id] = ed_ballots
            precinctMVBallots[area_id] = mv_ballots

with ZipFile("resultdata-raw.zip") as rzip:
    zipfilenames = get_zip_filenames(rzip)

    have_reg_by_party = "Registration.txt" in zipfilenames
    if have_reg_by_party:
        with TSVReader("Registration.txt",opener=rzip,binary_decode=True,
                    validate_header=REGISTRATION_header) as f:
            for (PrecinctName, PrecinctExternalId, ElectorGroupName,
                ElectorGroupExternalId, Count) in f.readlines():
                party_id = partyName2ID[ElectorGroupName]
                precinctPartyReg[f"PCT{PrecinctExternalId}:{party_id}"] = nocomma(Count)

    have_BallotGroupTurnout = "BallotGroupTurnout.psv" in zipfilenames
    if have_BallotGroupTurnout:
        allpctPartyTurnout = defaultdict(int) # Use int default
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
    else:
        # We don't have per-card turnout
        Card_Index2Id = { '*TOTALS*':'RSVot'}
        Counting_Group2Id = {'Vote by Mail':'MV'}
        if have_sov_turnout:
            allpctPartyTurnout['TO:RSVot:ALL'] = precinctBallots['ALL']
            allpctPartyTurnout['ED:RSVot:ALL'] = precinctEDBallots['ALL']
            allpctPartyTurnout['MV:RSVot:ALL'] = precinctMVBallots['ALL']
            for p in partyCodes[1:]:
                allpctPartyTurnout[f'TO:RSVot:{p}'] = None
                allpctPartyTurnout[f'ED:RSVot:{p}'] = None
                allpctPartyTurnout[f'MV:RSVot:{p}'] = None



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
    return precinctPartyReg.get(f"{area_id}:{NPPCrossover(party_id)}",None)

def appendNPPALL(cols:List[int]):
    """
    Compute the sum of xxxNPP columns and append as NPPALL
    """
    cols.append(sum([int(cols[i]) for i in npp_cols if cols[i]!=None]))

def cleancols(cols:List[int]):
    """
    Convert 0 list values to '' if all 0 except the first column
    """
    if len(cols)>1 and cols[0] and max(cols[1:])==0:
        cols = [v if v else '' for v in cols]
    return cols

with ZipFile("turnoutdata-raw.zip") as rzip:
    zipfilenames = get_zip_filenames(rzip)

    have_regstat = args.regstat and "regstat.tsv" in zipfilenames
    if have_regstat:
        with TSVReader("regstat.tsv",opener=rzip,binary_decode=True,
                    validate_header=REGSTAT_header) as f:
#            (Area, Total_Registration, Military_and_Overseas, Permanent_Mail_Voters,
#            American_Independent, Democratic, Green, Libertarian,
#            Peace_and_Freedom, Republican, No_Party_Preference_Unknown, Chinese,
#            English, Filipino, Hindi, Japanese, Khmer, Korean, Russian, Spanish,
#            Thai, Vietnamese, Other, Pre_registered_Under_18)

            for r in f.readdict():
                # Read records as dict
                area_id = r['Area']
                if area_id == 'district_all':
                    area_id = 'ALL'
                precinctPartyReg[f"{area_id}:ALL"] = nocomma(r['Total Registration'])
                for p,pname in partyNames.items():
                    if pname=='No Party Preference':
                        pname='No Party Preference/Unknown'
                    precinctPartyReg[f"{area_id}:{p}"] = nocomma(r[pname])

    if have_sov_turnout:
        # Override all party registration from SOV
        for area_id, total_registration in precinctReg.items():
            precinctPartyReg[f"{area_id}:ALL"] = total_registration

        with TSVReader("vbmprecinct.csv",opener=rzip,binary_decode=True,
                        validate_header=VBM_header) as f:
            VBM_turnout = list(f.readdict())

    if args.novbmprecinct:
        with TSVReader("vbmsummary.csv",opener=rzip,binary_decode=True,
                        trim_quotes='"') as f:
            # The vbmprecinct.tsv is invalid. Substitute vbmsummary
            VBM_summary = list(f.readdict())
            r = VBM_summary[-1]
            # The Totals are not included
            # The accepted column is missing
            for p in partyHeadings[1:]:
                r[p+'Pending'] = 0
                r[p+'Accepted'] = (int(r[p+'Returned'])
                                   -int(r[p+'Challenged'])-int(r[p+'Pending']))

            for group in groupHeadings:
                total = sum([int(r[p+group]) for p in partyHeadings[1:]])
                r[group] = total

            r['VotingPrecinctID'] = 'ALL'

            VBM_turnout.insert(0,r)


    with TSVWriter(TURNOUT_FILE,sort=True,sep=separator,
                   header=f"area_id|subtotal_type|result_stat|{partyCodeHeader2}") as o:
        foundpct = set()
        for r in VBM_turnout:
            pct = r['VotingPrecinctID']
            if pct=='ALL':
                pcta = pct
            else:
                pcta = 'PCT'+pct

            if pcta in foundpct:
                print(f"Skipping duplicate precinct {pcta}")
                continue
            foundpct.add(pcta)
            groupcols = []

            # Create a pseudo-group for party registration
            cols_to = [getPrecinctPartyReg(pcta, ph) for ph in partyCodes2]
            o.addline(pcta,'TO','RSReg',*cols_to)
            groupcols.append(cols_to)

            if precinctBallots:
                cols = [precinctBallots[pcta]]+None_party_cols
                o.addline(pcta,'TO','RSVot',*cols)
                groupcols.append(cols)


            if not args.novbmprecinct or pcta=='ALL':
                # Create computed election-day registration
                cols_mv = [int(r[ph+'Issued']) for ph in partyHeadings]
                if args.crossover:
                    appendNPPALL(cols_mv)
                cols_ed = [
                    None if to == None or mv==None else
                    int(to)-int(mv) if int(to)>=int(mv) else 0
                    for to,mv in zip(cols_to, cols_mv)]
                o.addline(pcta,'ED','RSReg',*cols_ed)
                groupcols.append(cols_ed)

            if precinctEDBallots:
                cols = [precinctEDBallots[pcta]]+None_party_cols
                o.addline(pcta,'ED','RSVot',*cols)
                groupcols.append(cols)

            if not args.novbmprecinct or pcta== 'ALL':
                for gh,group in zip(groupHeadings,groupCodes):
                    cols = [r[ph+gh] for ph in partyHeadings]
                    if args.crossover:
                        appendNPPALL(cols)
                    o.addline(pcta,'MV',group,*cols)
                    groupcols.append(cols)
            else:
                # Insert MV
                cols = [precinctMVBallots[pcta]]+None_party_cols
                o.addline(pcta,'MV','RSVot',*cols)
                groupcols.append(cols)

            if pcta == 'ALL':
                groupCodes2 = ['RSRegTO','RSVotTO','RSVotED','RSVot']
                continue

            if not args.novbmprecinct:
                addGroupCols('ALL')

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
            skip = 0
            if have_regstat and f"{area}:DEM" in precinctPartyReg:
               # Reset the computed summary area TO/ED registration
               i_RSRegTO = groupCodes2.index('RSRegTO')
               i_RSRegED = groupCodes2.index('RSRegED')
               i_RSRegMV = groupCodes2.index('RSIss')
               for i,p in enumerate(partyCodes2):
                   sdisttotal[area][i_RSRegTO][i] = t = nocomma(precinctPartyReg[f"{area}:{p}"])
                   m = nocomma(sdisttotal[area][i_RSRegMV][i])
                   if m and t and t>m:
                       sdisttotal[area][i_RSRegED][i] = t-m
            for i,group in enumerate(groupCodes2):
                m=re.match(r'(RS.*)(TO|ED)$',group)
                if m:
                    group, vg = m.groups()
                else:
                    vg = 'MV'
                line = o.joinline(area,vg,group,*cleancols(sdisttotal[area][i]))
                if area=='CONG13':
                    continue
                if area=='ALL':
                    if group=='RSVot' and have_BallotGroupTurnout:
                        skip += 1
                        continue
                    o.lines.insert(i-skip,line)
                else:
                    o.lines.append(line)

        # Insert turnout for ALL
        def getAllpctPartyTurnout(k):
            return allpctPartyTurnout[k] if k in allpctPartyTurnout else ''

        if have_BallotGroupTurnout:
            for i, vg in reversed(list(enumerate(Counting_Group2Id.values()))):
                for rs in reversed(list(Card_Index2Id.values())):
                    cols = [getAllpctPartyTurnout(f'{vg}:{rs}:{party_id}')
                            for party_id in partyCodes]
                    #if rs.startswith('RSCd'):
                        #cols.append('')
                    #else:
                        #appendNPPALL(cols)
                    if args.crossover:
                        appendNPPALL(cols)
                    o.lines.insert(i+1,o.joinline('ALL',vg,rs,*cols))

        o.sort = False

    with TSVWriter('sdistpct.tsv',sort=False,sep=separator,
                   header="area_id|precinct_ids") as o:
        for area,pcts in sorted(sdistpct.items(), key=sortkey):
            o.addline(area,' '.join(pcts))
    with TSVWriter('pctsdist.tsv',sort=False,sep=separator,
                   header="precinct_ids|area_ids") as o:
        for area,pcts in sorted(pctsdist.items(), key=sortkey):
            o.addline(' '.join(pcts),area)









