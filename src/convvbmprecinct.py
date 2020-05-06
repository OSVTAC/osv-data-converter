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
import distpctutils

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
    parser.add_argument('-Z', dest='zero', action='store_true',
                        help='make a zero report')
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

if args.zero:
    OUT_DIR = "../out-orr/resultdata-zero"
    TURNOUT_FILE = f"{OUT_DIR}/turnout.tsv"


# Load the distpct.tsv file
# Get the precincts in neighborhoods
precinctNeigh = {}
distpct = distpctutils.load_distpct("../ems/distpct.tsv.gz")
for district_id, precinct_set in distpct.items():
    if not district_id.startswith('NEIG'):
        continue
    for pct in precinct_set.split():
        precinctNeigh[pct] = district_id


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
    crossoverPartySuff = "AINPP|DEMNPP|LIBNPP|NPPALL".split("|")

else:
    partyCodes2 = partyCodes = ["ALL"]+list(partyNames.keys())
    partyCodeHeader2 = partyCodeHeader = "|".join(partyCodes)
    partyHeadings = "|American_Independent_|Democratic_|Green_|Libertarian_|Peace_&_Freedom_|Republican_|No_Party_Preference_".split('|')
    crossoverPartySuff = []

None_party_cols = [None]*(len(partyCodes2)-1)


# Add NPPALL as computed total of AINPP+DEMNPP+LIBNPP+NPP
nppall_i = len(partyCodes)
npp_cols = [i for i,n in enumerate(partyCodes) if n.endswith('NPP')]

#print(f"npp_cols={npp_cols}")


groupCodes = "RSReg|RSCst|RSCnt|RSPnd|RSCha".split('|')
groupHeadings = "Issued|Returned|Accepted|Pending|Challenged".split('|')

RSheaderCards = "RSReg|RSCst|RSCnt|RSCd1|RSCd2|RSPnd|RSCha"
RSheader = "RSReg|RSCst|RSCnt|RSPnd|RSCha"

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

def set_total(k,v):
    """
    Sets turnout and have_total for the key and value
    """
    turnout[k] = have_total[k] = v

def add_total(k, v):
    if k not in have_total:
        turnout[k] += v

def set_total_default(k, v):
    if k not in turnout:
        turnout[k] = have_total[k] = v

def set_precinct_sum(area:str, pc, vg_rs, v, sdists, skipAll=False):
    """
    Set the precinct-level turnout and sum for summary districts
    """
    # Set the base value
    set_total_default(f"{area}:{pc}:{vg_rs}", v)

    sumpctall = pc.endswith('NPP') and args.crossover
    if sumpctall:
        add_total(f"{area}:NPPALL:{vg_rs}", v)

    for area in sdists:
        add_total(f"{area}:{pc}:{vg_rs}", v)
        if sumpctall:
            add_total(f"{area}:NPPALL:{vg_rs}", v)

def nocomma(s:str):
    """
    convert to int with commas removed
    """
    if isinstance(s, str):
        s = int(s.replace(',',''))
    return s

def sortkey(k):
    order=ordermap.get(k[0:4],' ')
    return order+k[4:] if len(k)>5 else order+'0'+k[4:]

def dsortkey(k):
    return sortkey(k[0])

Counting_Group2Id = {
    '*TOTALS*':'TO',
    'Election Day':'ED',
    'Vote by Mail':'MV',
    }

voting_groups = Counting_Group2Id.values()

Card_Index2Id = {
    '*TOTALS*':'RSCnt',
    '1':'RSCd1',
    '2':'RSCd2',
    }

# turnout['area:party:group:stat']={"RSReg|RSCst|RSCnt|RSPnd|RSCha"}
turnout = defaultdict(int)
have_total = {} #Marker for set totals


allpcts = set()
allpcts.add('ALL')

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

        for cols in f.readlines():
            if format2:
                (area_id, total_registration, total_ballots,
                 ed_ballots, mv_ballots) = cols
            else:
                (area_id, total_registration, ed_registration, mv_registration,
                 total_ballots, ed_ballots, mv_ballots) = cols

            set_total(f'{area_id}:ALL:TO:RSReg', nocomma(total_registration))
            set_total(f'{area_id}:ALL:TO:RSCnt', nocomma(total_ballots))
            set_total(f'{area_id}:ALL:ED:RSCnt', nocomma(ed_ballots))
            set_total(f'{area_id}:ALL:MV:RSCnt', nocomma(mv_ballots))
            if args.novbmprecinct:
                allpcts.add(area_id)
            else:
                for vg in ['TO','ED']:
                    set_total(f'{area_id}:ALL:{vg}:RSCst', turnout[f'{area_id}:ALL:{vg}:RSCnt'])

with ZipFile("resultdata-raw.zip") as rzip:
    zipfilenames = get_zip_filenames(rzip)

    have_reg_by_party = "Registration.txt" in zipfilenames
    if have_reg_by_party:
        with TSVReader("Registration.txt",opener=rzip,binary_decode=True,
                    validate_header=REGISTRATION_header) as f:
            for (PrecinctName, PrecinctExternalId, ElectorGroupName,
                ElectorGroupExternalId, Count) in f.readlines():
                party_id = partyName2ID[ElectorGroupName]
                allpcts.add("PCT"+PrecinctExternalId)
                set_total(f"PCT{PrecinctExternalId}:{party_id}:TO:RSReg", nocomma(Count))

    have_BallotGroupTurnout = "BallotGroupTurnout.psv" in zipfilenames
    if have_BallotGroupTurnout:
        RSheader = RSheaderCards
        # We will overwrite the prior ALL totals summed here
        for vg in voting_groups:
            turnout[f'ALL:ALL:{vg}:RSCnt'] = 0

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
                k = f'ALL:{party_id}:{voting_group}:{card_id}'
                set_total(k, turnout[k] + int(Turnout))


            # Fix missing card totals
            for rs in ['RSCd1','RSCd2']:
                for p in partyCodes:
                    turnout[f'ALL:{p}:TO:{rs}']= (
                        turnout[f'ALL:{p}:ED:{rs}']+
                        turnout[f'ALL:{p}:MV:{rs}'])

RSCodes = RSheader.split('|')

def NPPCrossover(party_id:str)->str:
    """
    Convert xxxNPP and NPPALL crossover ID to just NPP for registration
    """
    return 'NPP' if (party_id.endswith("NPP")
                    or party_id.startswith("NPP")) else party_id

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
                to_key = f"{area_id}:ALL:TO:RSReg"
                if to_key not in turnout:
                    turnout[to_key] = nocomma(r['Total Registration'])
                for p,pname in partyNames.items():
                    if pname=='No Party Preference':
                        pname='No Party Preference/Unknown'
                    to_key = f"{area_id}:{p}:TO:RSReg"
                    if to_key not in turnout:
                        turnout[f"{area_id}:{p}TO:RSReg"] = nocomma(r[pname])

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


    with TSVWriter(TURNOUT_FILE,sort=False,sep=separator,
                   header=f"area_id|subtotal_type|party|"+RSheader) as o:
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
                allpcts.add(pcta)

            sdists = []
            if pcta!='ALL':
                # Create reverse map precinct to summary groups
                for h,c in distHeadMap.items():
                    area = precinctNeigh[pct] if c=='NEIG' else c+r[h]
                    if area=='CONG13':
                        continue
                    if area not in sdistpct:
                        sdistpct[area] = []

                    sdistpct[area].append(pct)
                    sdists.append(area)

                # Form a space separated list
                sdist_group = ' '.join(sorted(sdists, key=sortkey))
                if sdist_group not in pctsdist:
                    pctsdist[sdist_group] = []
                pctsdist[sdist_group].append(pct)
                sdists.append('ALL')


            if not args.novbmprecinct or pcta=='ALL':
                # Create computed election-day registration
                for pc, ph in zip(partyCodes, partyHeadings):
                    mv_reg = int(r[ph+'Issued'])
                    to_key = f"{pcta}:{pc}:TO:RSReg"
                    if to_key not in turnout:
                        continue
                    to_reg = turnout[to_key]
                    ed_reg = to_reg - mv_reg
                    if ed_reg<0:
                        ed_reg = 0
                    set_precinct_sum(pcta, pc, "TO:RSReg", to_reg, sdists)
                    set_precinct_sum(pcta, pc, "ED:RSReg", ed_reg, sdists)

                for pc, ph in zip(partyCodes, partyHeadings):
                    for gh,gc in zip(groupHeadings,groupCodes):
                        v = int(r[ph+gh])
                        if gc=='RSCst' and pc=='ALL':
                            # Reset the total ballots case
                            k = f'{pcta}:ALL'
                            if k+":ED:RSCst" in turnout:
                                turnout[k+":TO:RSCst"] = turnout[k+":ED:RSCst"] + v
                        set_precinct_sum(pcta, pc, "MV:"+gc, v, sdists)

        # Compute the TO/ED RSCst, RSPnd, RSCha from MV totals
        pclist = partyCodes if have_BallotGroupTurnout else ['ALL']
        for pc in pclist:
            # The ED:RSCst is the same as ED:RSCnt
            #import pdb; pdb.set_trace()
            v = turnout.get(f'ALL:{pc}:ED:RSCnt', None)
            if v==None:
                continue
            set_total_default(f'ALL:{pc}:ED:RSCst', v)
            # Compute the TO:CST
            v2 = turnout.get(f'ALL:{pc}:MV:RSCst', None)
            if v2==None:
                continue
            set_total(f'ALL:{pc}:TO:RSCst', v + v2)
            # Copy the RSPnd/RSCha
            for rs in ['RSPnd','RSCha']:
                v = turnout.get(f'ALL:{pc}:MV:{rs}', None)
                if v==None:
                    continue
                set_total_default(f'ALL:{pc}:TO:{rs}', v)

        if args.crossover:
            # Compute NPPALL
            for rs in "RSCst RSCnt RSCd1 RSCd2 RSPnd RSCha".split():
                for vg in ['TO','ED']:
                    if f'ALL:DEMNPP:{vg}:{rs}' not in turnout:
                        continue
                    set_total_default(f'ALL:NPPALL:{vg}:{rs}',sum(
                        [ turnout[f'ALL:{pc}:{vg}:{rs}'] for pc in
                          partyCodes if pc.endswith('NPP')]))

        # Output all+precinct lines
        sdists = sorted(sdistpct.keys(), key=sortkey)
        for area in sorted(allpcts)+sdists:
            for vg in voting_groups:
                foundLast = None
                for ph in partyCodes2:
                    pref = f"{area}:{ph}:{vg}:"
                    #print(f'turnout[{pref+"RSReg"}]={turnout.get(pref+"RSReg",None)}')
                    if (pref+"RSReg" in turnout or pref+"RSCst" in turnout or
                        pref+"RSCnt" in turnout) :
                        cols = ["" if pref+rs not in turnout else
                                0 if rs!='RSReg' and args.zero else
                                turnout.get(pref+rs,"") for rs in RSCodes]
                        o.addline(area,vg,ph,*cols)
                        foundLast = ph
                    elif foundLast=="ALL":
                        o.addline(area,vg,"*")
                        break

    with TSVWriter('sdistpct.tsv',sort=False,sep=separator,
                   header="area_id|precinct_ids") as o:
        for area,pcts in sorted(sdistpct.items(), key=dsortkey):
            o.addline(area,' '.join(pcts))
    with TSVWriter('pctsdist.tsv',sort=False,sep=separator,
                   header="precinct_ids|area_ids") as o:
        for area,pcts in sorted(pctsdist.items(), key=dsortkey):
            o.addline(' '.join(pcts),area)









