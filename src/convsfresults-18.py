#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2018-2020  Carl Hage
#
# This file is part of Open Source Voting Data Converter (ODC).
#
# ODC is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

"""
Program to convert results download datasets for SF to ORR data format

Converts 2018 SF SOV format
"""

# Library References
import json
import logging
import os
import os.path
import re
import argparse
import struct
import operator, functools

# Local file imports
from tsvio import TSVReader
from re2 import re2
from translations import Translator

# Library imports
from datetime import datetime
from collections import OrderedDict, namedtuple, defaultdict
from typing import List, Pattern, Match, Dict, Union
from zipfile import ZipFile

DESCRIPTION = """\
Converts election results data from downloaded from
sfgov.org Election Results - Detailed Reports

Reads the following files:
  * resultdata-raw.zip - Downloaded SF results
  * [TODO] ../election.json - Election definition data
  * [TODO] config-odc.yaml - Configuration file with conversion options

Creates the following files:
  * results.json
  * results-{contest_id}.tsv
  * A set of intermediate files extracted from the resultdata-raw

"""
#TODO:
# -

###################################

# Types defined to document usage

Area_Id = str           # PCTnnn or district code representing a geographic area
Precinct_Id = str       # Precinct Id without the PCT prefix
Result_Stat_Id = str    # RSTot, RSCst, etc.
Voting_Group_Id = str   # ED (Election Day) MV (Vote by Mail) TO (Total) count group
Contest_Id = str        # Primary Id for a contest

###################################
VERSION='0.0.3'     # Program version
RESULTS_FORMAT = '0.3'

# These enable deprecated results format
ADD_RESULTS_VECTOR=False    # True=include results json vector per choice
ADD_CHOICES=False           # True=include

SF_ENCODING = 'ISO-8859-1'
SF_SOV_ENCODING ='UTF-8'
SF_HTML_ENCODING = 'UTF-8'

OUT_DIR = "../out-orr/resultdata"
TRANSLATIONS_FILE = (os.path.dirname(__file__)+
                     "/../submodules/osv-translations/translations.json")


DEFAULT_JSON_DUMP_ARGS = dict(sort_keys=True, separators=(',\n',':'), ensure_ascii=False)
PP_JSON_DUMP_ARGS = dict(sort_keys=True, indent=4, ensure_ascii=False)

approval_fraction_pat = re2(r'^(\d+)/(\d+)$')
approval_percent_pat = re2(r'^(\d+)%$')

# Result Stats by type
resultlistbytype = {}
for line in """\
CW=RSReg RSCst RSRej RSOvr RSUnd RSTot RSWri
RW=RSReg RSCst RSRej RSOvr RSUnd RSExh RSTot RSWri
C=RSReg RSCst RSRej RSOvr RSUnd RSTot""".split('\n'):
    name, resnames = line.split('=')
    resultlistbytype[name] = resnames.split(' ')

winning_status_names = {
    'W':'winning', 'X':'winning_failed_recall', 'T':'tied', 'C':'tied_winner',
    'D':'tied_not_winner', 'R': 'to_runoff', 'S':'tied_selected_for_runoff',
    'E':'rcv_eliminated', 'N':'not_winning', '':''
}

VOTING_STATS = OrderedDict([
    ('RSTot', 'Total Votes'),       # Sum of valid votes reported
    ('RSCst', 'Ballots Cast'),      # Ballot sheets submitted by voters
    ('RSReg', 'Registered Voters'), # Voters registered for computing turnout
    ('RSEli', 'Eligible Voters'),   # Voters eligible to be registered
    ('RSTrn', 'Voter Turnout'),     # (SVCst/SVReg)*100
    ('RSRej', 'Ballots Rejected'),  # Not countable
    ('RSUnc', 'Ballots Uncounted'), # Not yet counted or needing adjudication
    ('RSWri', 'Writein Votes'),     # Write-in candidates not explicitly listed
    ('RSUnd', 'Undervotes'),        # Blank votes or additional votes not made
    ('RSOvr', 'Overvotes'),         # Possible votes rejected by overvoting
    ('RSExh', 'Exhausted Ballots')  # All RCV choices were eliminated (RCV only)
    ])
# These district subtotals are meaningless
countywide_districts = {'0','SEN11'}

rsnamemap = {'Registration':'RSReg',
             'Ballots Cast':'RSCst',
             'Turnout (%)':'RSTrn',     # Unused turnout percent
             'WRITE-IN':'RSWri',
             'Under Vote':'RSUnd',
             'Over Vote':'RSOvr' }

distcodemap = {'Congressional':'CONG',
               'Senatorial':'SEN',
               'Assembly':'ASSM',
               'BART':'BART',
               'Supervisorial':'SUPV',
               'City':''}

class FormatError(Exception):pass # Error matching expected input format

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
    parser.add_argument('-P', dest='pretty', action='store_true',
                        help='pretty-print json output')
    parser.add_argument('-Z', dest='zero', action='store_true',
                        help='make a zero report')
    parser.add_argument('-s', dest='dirsuffix',
                        help='set the output directory (../out-orr default)')
    parser.add_argument('-z', dest='withzero', action='store_true',
                        help='include precincts with zero voters')

    args = parser.parse_args()

    return args

def unpack(
    fmt:str,     # Format string
    line:bytes     # Line to unpack
    ) -> List:
    """
    Unpacks a fixed length record into a list of strings, numbers, or bools
    """
    return [x.decode(SF_ENCODING).strip() if type(x)==bytes else x
            for x in struct.unpack(fmt, line.strip())]

def boolstr(x) -> str:
    """
    Converts a boolean value to Y/N or blank for None
    """
    return '' if x == None else 'Y' if x=='1' or x=='Y' or x==True else 'N'

def id4(x:str)->str:
    """
    Truncates a numeric ID to last 4 characters
    """
    return x[-4:]

def id3(x:str)->str:
    """
    Truncates a numeric ID to last 3 characters
    """
    return x[-3:]

def strnull(x:str)->str:
    """
    Maps None to ""
    """
    return "" if x == None else x

def dict_append(d:Dict, k, v):
    """
    Same as d[k].append(v) but works if k is not in d
    """
    d.setdefault(k,[]).append(v)

def dict_extend(d:Dict, k, v):
    """
    Same as d[k].extend(v) but works if k is not in d
    """
    d.setdefault(k,[]).extend(v)

def dict_add(d:Dict, k, v:int):
    """
    Same as d[k]+=v but works if k is not in d
    """
    if v==None:
        return
    d[k] = d.get(k,0) + v

intQ = Union[int,None]

def intNone(v:str)->intQ:
    """
    Convert null string to None
    """
    return None if v=='' else int(v)

def addQ(a:intQ,b:intQ)->intQ:
    """
    Add with values that can be none
    """
    return None if a==None or b==None else a+b

def subQ(a:intQ,b:intQ)->intQ:
    """
    Subtract wiht values that can be none
    """
    return None if a==None or b==None else a-b

args = parse_args()

if args.dirsuffix:
    OUT_DIR = OUT_DIR+'-'+args.dirsuffix
elif args.zero:
    OUT_DIR = OUT_DIR+'-zero'

if not os.path.exists(OUT_DIR):
    os.makedirs(OUT_DIR)

# Load translations
translator = Translator(TRANSLATIONS_FILE)

json_dump_args = PP_JSON_DUMP_ARGS if args.pretty else DEFAULT_JSON_DUMP_ARGS

separator = "|" if args.pipe else "\t"

# Save registration by subtotal and precinct
# RSRegSave['MV']['PCT1101'] has registration for vote by mail in precinct 1101
RS_Area_Table = Dict[Area_Id,int]
RS_VG_Table = Dict[Voting_Group_Id,RS_Area_Table]

RSRegSave:RS_VG_Table = dict(TO={},ED={},MV={})
RSRegSave_MV:RS_Area_Table = RSRegSave['MV']
RSRegSave_ED:RS_Area_Table= RSRegSave['ED']
RSRegSave_TO:RS_Area_Table = RSRegSave['TO']
RSCstSave_MV:RS_Area_Table = {} # VBM returned

RSRegSave_MV['CONG13']=RSRegSave_ED['CONG13']=RSRegSave_TO['CONG13']=0

CrossoverParties = {'DEM','AI','LIB','NPP'}
# Load turnout by party
turnoutfile = f'{OUT_DIR}/turnout.tsv'
partyTurnout = []
have_turnout = os.path.isfile(turnoutfile)
if have_turnout:
    with TSVReader(turnoutfile) as f:
        # cols area_id|subtotal_type|result_stat|ALL|AI|...
        # f.header is contains party headings for cols[3:]
        partyHeaders = f.header[3:]
        partyTurnout.append('\t'.join(f.header))
        for cols in f.readlines():
            area_id, subtotal_type, result_stat = cols[:3]
            if area_id=='ALL':
                partyTurnout.append('\t'.join(cols))

            if result_stat=='RSReg':
                if subtotal_type=='TO':
                    rs = RSRegSave_TO
                else:
                    # Skip ED
                    continue
            elif result_stat=='RSIss':
                rs = RSRegSave_MV
            elif result_stat=='RSCst':
                rs = RSCstSave_MV
            else:
                continue
            for i, party in enumerate(partyHeaders):
                if party=='ALL':
                    party=''
                rs[area_id+party]=v=intNone(cols[3+i])
                if party.endswith('NPP'):
                    if party != 'NPP':
                        party = party[:-3]
                if party in CrossoverParties:
                    dict_add(rs, area_id+party+'ALL', v)

def putfile(
    filename: str,      # File name to be created
    headerline: str,    # First line with field names (without \n)
    datalist: List[str] # List of formatted lines with \n included
    ):
    """
    Opens a file for writing, emits the header line and sorted data
    """
    with open(filename,'w') as outfile:
        if separator != "|":
            headerline = re.sub(r'\|', separator, headerline)
        outfile.write(headerline+'\n');
        outfile.writelines(sorted(datalist))

def jointsvline(
    *args)->str:
    """
    Join a list of columns with \t and append \n
    """
    return(separator.join(map(str,args))+'\n')

def newtsvline(
    datalist: List[str],    # List to build
    *args):
    """
    Join a list of columns with \t and append \n, the add to datalist
    """
    line = jointsvline(*args)
    datalist.append(line)
    return(line)

def newtsvlineu(
    foundHash: Dict[str, str],   # Hash of unique lines by args[0]
    datalist: List[str],    # List to build
    errorPrefix: str,       # Duplicate error prefix
    *args):
    """
    Calls newtsvline with a check for unique ID in args[0]
    """
    global linenum
    line = jointsvline(*args)
    if not args[0] in foundHash:
        foundHash[args[0]] = line
        datalist.append(line)
    elif line !=foundHash[args[0]]:
        print("{errorPrefix} {linenum}:\n {foundHash[args[0]]}  {line}" )

def decodeline(line, encoding=SF_ENCODING):
    global linenum
    linenum += 1
    try:
        line = line.decode(encoding).strip()
    except:
        print(f"Can't decode {linenum}:{line}")
        return ""
    return line

def candnametrim(name:str):
    """
    Trim party prefix and WRITE-IN from a candidate name
    """
    m = re.match(r'(?:(\S\S\S?) - )?(WRITE-IN )?(.+)', name)
    if m:
        return m.groups()
    else:
        return ('','',name)

def  flushcontest(contest_order, contest_id, contest_name,
                  headerline, contest_rcvlines,
                  contest_totallines, contest_arealines):
    """
    Write out the results detail file for a contest
    """
    filename = f'{OUT_DIR}/results-{contest_id}.tsv'
    with open(filename,'w') as outfile:
        if separator != "|":
            headerline = re.sub(r'\|', separator, headerline)
        outfile.write(headerline);
        if contest_rcvlines:
            outfile.writelines(contest_rcvlines)
        outfile.writelines(contest_totallines)
        outfile.writelines(contest_arealines)

re2c = re2('')

#RW=RSReg RSCst RSRej RSOvr RSUnd RSExh RSTot RSWri
#    x     x     x      0    1      2     3     4
rcvLabelMap = {
    'WRITE-IN':4,
    'Exhausted by Over Votes':0,
    'Under Votes':1,
    'Exhausted Ballots':2,
    'Continuing Ballots':3,
    'TOTAL':-1,
    'REMARKS':-1,
    '':-1
    }

def checkDuplicate(d:Dict[str,str],  # Dict to set/check
                   key:str,          # Key
                   val:str,          # Value
                   msg:str):         # Message on duplicate
    global linenum
    if key not in d or d[key]=="0":
        d[key] = val
    elif d[key] != val:
        print(f"Duplicate {msg} for {key}->{val}!={d[key]} at {linenum}")
        raise

def loadEligible()->int:
    """
    Reads the ../vr/county.tsv file to locate eligible voters by county.
    Returns the count as a string, a numbmer or null.
    """
    try:
        with TSVReader("../vr/county.tsv") as reader:
            for l in reader.readlines():
                if l[0] == "San Francisco":
                    return int(l[1])
    except Exception as ex:
        pass
    return ""

def loadRCVData(rzip,                   # zipfile context
                contest_name:str,       # Contest name
                candnames:List[str],    # List of candidate names
                statprefix:List[str],   # Stats from grand total report
                )->List[str]:
    """
    Load the html file with RCV rounds and prepare the result data lines.
    """
    filename = contest_name.lower()
    # Pattern match the file names
    if (re2c.sub2ft(filename, r'.*supervisor\D+(\d+)$', 'd{0}.html') or
        re2c.sub2ft(filename, r'^.*(mayor|assessor|defender).*$', '{0}.html')):
        filename = re2c.string
    else:
        raise FormatError(f"Unmatched RCV contest name")


    rcvtable = [ [] for i in range(len(candnames) + 5) ]
    rcvlines = []
    with rzip.open(filename) as f:
        html = f.read().decode(SF_HTML_ENCODING)
        m = re.search(r'<table[^<>]*>\s*(.*)</table',html,re.S)
        if not m:
            raise FormatError(f"Unmatched RCV html in {filename}")

        i = 0
        for row in m[1].split('</tr>'):
            if re.search('<th',row):
                # Skip the header
                continue
            row = re.sub(r'(\s+|&nbsp;)+',' ',row) # Replace newline with space
            row = re.sub(r'</td>\s*$','',row)    #trim end of last td
            cols = [re.sub(r'.*<td[^<>]*>\s*','',cell, re.S)
                    for cell in row.split('</td>')]
            #print("RCVline:"+'|'.join(cols))

            candname = cols[0]

            if candname=='REMARKS': break

            if candname in rcvLabelMap:
                j = rcvLabelMap[candname]
                if j < 0: continue
            else:
                (party_name, writein, candname) = candnametrim(cols[0])
                if candnames[i] != candname:
                    raise FormatError(
                     f"Unmatched RCV candidate {candname} in {filename}")
                j = i + 5
                i+=1
            rcvtable[j] = cols[1::3]
            #print(f"{candname}:{rcvtable[j]}")
        # End loop over html table rows

        # Transpose the data to decreasing RCV rounds
        rcvrounds = len(rcvtable[3])

        if args.zero:
            rcvrounds = 1
            final_cols = cols = ['RCV1']+statprefix+['0']*len(rcvtable)
            newtsvline(rcvlines, *cols)
            return rcvlines, final_cols

        # Check duplicate
        if rcvrounds>1:
            dup = 1
            for j in range(len(rcvtable)):
                if rcvtable[j][0] != rcvtable[j][1]:
                    dup = 0
                    break
            if dup==0:
                print(f"Not duplicated: {contest_name}\n")
        else:
            dup = 0

        for i in range(rcvrounds,dup,-1):
            area_id = f'RCV{i-dup}'
            cols = [area_id]+statprefix+[ (rcvtable[j][i-1]
                                        if i<=1+dup or rcvtable[j][i-1] != '0'
                                        else '')
                                         for j in range(len(rcvtable)) ]
            if i==rcvrounds:
                final_cols = cols
            newtsvline(rcvlines, *cols)
        # End loop over rcv rounds
    # End processing html file
    return rcvlines, final_cols

total_registration = 0
total_precinct_ballots = 0
total_mail_ballots = 0
total_precincts = 0

isrcv = set() # Contest IDs with RCV

no_voter_precincts = set() # IDs for precincts with no registered voters

#Load the contest ID map
# TODO: Remap to original DFM IDs
# Temporary - Use Omniballot contest ID, EDS candidate ID
have_contmap = os.path.isfile("contmap.tsv")
if have_contmap:
    with TSVReader("contmap.tsv") as r:
        contmap = r.load_simple_dict(0,1)
    with TSVReader("candmap.tsv") as r:
        candmap = r.load_simple_dict(0,1)
have_contmap = os.path.isfile("../omniballot/contmap.tsv")
if have_contmap:
    with TSVReader("../omniballot/contmap.tsv") as r:
        # Map ID to omniballot_id
        contmap_omni = r.load_simple_dict(1,0)
else:
    contmap_omni = {}

have_runoff = os.path.isfile("../omniballot/runoff-omni.tsv")
if have_runoff:
    with TSVReader("../omniballot/runoff-omni.tsv") as r:
        runoff_by_contid = r.loaddict(2)
else:
    runoff_by_contid = {}

#Load countywide eligible voters
eligible_voters = loadEligible()

#Load approval_required
with TSVReader("../omniballot/contlist-omni.tsv") as r:
    approval_required_by_omni_id = r.load_simple_dict(3,7)

#print("approval_required=",approval_required_by_omni_id)

# Process the downloaded SF results stored in resultdata-raw.zip
with ZipFile("resultdata-raw.zip") as rzip:
    rfiles = rzip.infolist()

    # Create a set with the zip file names
    zipfilenames = set()
    for info in rfiles:
        zipfilenames.add(info.filename)

    # Read turnout details


    # Read turnout details

    # Read the release ID and title
    results_id = ''
    results_title = ''
    if args.zero:
        results_id = 'zero'
        results_title = 'Zero Report'
    elif "lastrelease.txt" in zipfilenames:
        with rzip.open("lastrelease.txt") as f:
            last_release_line = f.read().decode('utf-8').strip()
            m = re.match(r'(.*):(.*)', last_release_line)
            if m:
                results_id, results_title = m.groups()
            else:
                print(f'Unmatched lastrelease.txt:{last_release_line}')

    # Compute status
    results_status = ('Preliminary' if re.search(r'(Preliminary|Night)',
                            results_title,flags=re.I) else
                        'Final' if re.search(r'(Final)',
                            results_title,flags=re.I) else
                        'Zero' if re.search(r'(Zero)',
                            results_title,flags=re.I) else 'Unknown')

    # Output file struct
    results_contests = []
    results_json = {
        '_results_format':RESULTS_FORMAT,
        "_reporting_time": datetime.now().isoformat(timespec='seconds',sep=' '),
        "_results_id": results_id,
        "_results_status": results_status,
        "_results_title": translator.get(results_title,context="results_title"),
        "turnout": {},
        'contests': results_contests,
        }

    # Get the ID maps in masterlookup.txt
    # Has ID maps only for RCV contests
    with rzip.open("masterlookup.txt") as f:
        candlist = []
        contlist = []
        pctlist = []
        tallylist = []
        linenum = 0
        for line in f:
            linenum += 1
            (rectype, id_, desc, listorder, contest_id, is_writein, is_prov
             ) = unpack("10s7s50s7s7sss", line)
            if rectype == "Candidate":
                newtsvline(candlist, id3(contest_id), id3(listorder), id3(id_),
                    desc, boolstr(is_writein))
            elif rectype == "Contest":
                contest_id = id3(id_)
                newtsvline(contlist, contest_id, desc)
                isrcv.add(contest_id)
            elif rectype == "Tally Type":
                #print(f'r={rectype} id={id_} d={desc} w={is_writein} p={is_prov}')
                newtsvline(tallylist, id3(id_), desc, boolstr(is_prov))
            elif rectype == "Precinct":
                newtsvline(pctlist, id4(id_), desc)
            else:
                raise FormatError(
                    f"Bad rectype '{rectype}' in masterlookup.txt:{linenum}")

        putfile("contlist-rcve.tsv",
                "contest_id|contest_name",
                contlist)
        putfile("candlist-rcve.tsv",
                "contest_id|display_order|candidate_id|candidate_name|is_writein",
                candlist)
        putfile("pctlist-rcve.tsv",
                "precinct_id|precinct_name",
                pctlist)
        putfile("tallytypes-rcve.tsv",
                "tallytype_id|tally_name",
                tallylist)

    # Split ID maps and result data in summary.txt
    header = "CONTEST_ID\tCONTEST_ORDER\tCANDIDATE_ORDER\tTOTAL\tCANDIDATE_PARTY_ID"\
    "\tCANDIDATE_ID\tVOTE_FOR\tCONTEST_TYPE\tCANDIDATE_TYPE\tTOTAL_PRECINCTS"\
    "\tPROCESSED_DONE\tPROCESSED_STARTED\tIS_WRITEIN_CANDIDATE"\
    "\tCONTEST_FULL_NAME\tCANDIDATE_FULL_NAME\tCONTEST_TOTAL\tundervote"\
    "\tovervote\tIS_WINNER\tcf_cand_class\tIS_PRECINCT_LEVEL\tPRECINCT_NAME"\
    "\tis_visible"

    if args.verbose:
        print("Reading summary.txt")
    skip = False
    with rzip.open("summary.txt") as f:
        line = f.readline().decode(SF_ENCODING).strip()
        if line != header:
            raise FormatError(f"Header mismatch:\n'{line}' should be\n'{header}")
        linenum = 1
        foundcont = {}
        foundcand = {}
        foundparty = {}
        contestname2id = {}
        contestcands = {} # Table of contests by ID to candidate name to ID dict
        candorder = {} # Candidate order by ID
        candlist = []
        writein_candlist = []
        contlist = []
        partylist = []
        contresults = []
        candresults = []
        precinct_count = {}
        contestvotefor = {} # Vote for number by contest
        contestorder = {}   # Order index by contest
        contesttype = {}    # Type code by contest
        contest_status = {} # Collected json data by contest_id_eds
        pctname2areaid = {} # Map original precinct ID to area ID
        pctturnout_reg = {} # Map original precinct ID to registration
        pctturnout_ed = {}  # Map original precinct ID to ED ballots
        pctturnout_mv = {}  # Map original precinct ID to MV ballots

        for line in f:
            line = decodeline(line)
            if line == "":
                continue

            (contest_id, contest_order, candidate_order, total, candidate_party_id,
            candidate_id, vote_for, contest_type, candidate_type, total_precincts,
            processed_done, processed_started, is_writein_candidate,
            contest_full_name, candidate_full_name, contest_total, undervote,
            overvote, is_winner, cf_cand_class, is_precinct_level, precinct_name,
            is_visible) = line.split('\t')

            if args.zero:
                processed_done = total = contest_total = 0
                undervote = overvote = 0

            if contest_id == "0":
                # Registration & Turnout has county total
                if candidate_id == "1":
                    total_precinct_ballots = int(total)
                else:
                    total_mail_ballots = int(total)
                total_registration = contest_total
                total_precincts_reporting = processed_done
                continue


            # Form order and IDs into fixed width leading 0 numbers
            (contest_id, contest_order, candidate_order, candidate_id
             ) =[ x.zfill(3) for x in
                 [contest_id, contest_order, candidate_order, candidate_id]]

            # Form party abbr from name and create code table
            # Strip WRITE-IN prefix on named candidates
            (party_name, writein, candidate_full_name
             ) = candnametrim(candidate_full_name)

            m = re.match(r'(\S\S\S?) - (.+)', candidate_full_name)
            if party_name:
                if party_name in foundparty:
                    if foundparty[party_name] != candidate_party_id:
                        print("Mismatched {party_name} party at {linenum}: {foundparty[party_name]} != {candidate_party_id}/")
                else:
                    foundparty[party_name] = candidate_party_id
                    newtsvline(partylist, candidate_party_id, party_name)

            elif candidate_party_id != "":
                print("Party {candidate_party_id} used with {candidate_full_name} at {linenum}")

            if writein:
                # Bug in summary - is_writein_candidate only for "WRITE-IN"
                is_writein_candidate = "1"

            # Convert contest type?

            contline = jointsvline(contest_order, contest_id,
                            contest_full_name, vote_for, contest_type)

            contres = jointsvline(contest_id, total_precincts,
                            processed_done, processed_started,
                            contest_total, undervote, overvote)

            contall = contline + contres

            if contest_id in foundcont:
                if foundcont[contest_id] != contall and is_writein_candidate != "1":
                    print(f"Mismatched contest at {linenum}:\n  {foundcont[contest_id]} !=\n  {contall}")
            else:
                foundcont[contest_id] = contall
                # contlist.append(contline) We will postpone until sov to get ids
                contresults.append(contres)
                if contest_full_name in contestname2id:
                    print(f'Duplicate contest name {contest_full_name} at {linenum}')
                contestname2id[contest_full_name] = contest_id
                # Initialize candidate dict
                contestcands[contest_id] = candname2id = {}
                contestvotefor[contest_id] = int(vote_for)
                contesttype[contest_id] = contest_type
                contestorder[contest_id] = contest_order
                precinct_count[contest_id] = int(total_precincts)

                # Prepare the results.json
                contest_status[contest_id] = conteststat = {
                    'precincts_reporting': int(processed_done),
                    #'reporting_time': '',
                    'total_precincts': int(total_precincts)
                    }
                if ADD_CHOICES:
                    conteststat['choices'] = []
                if ADD_RESULTS_VECTOR:
                    conteststat['result_stats'] = []


            candline = jointsvline(contest_id, candidate_order, candidate_id,
                           candidate_type,
                            candidate_full_name,
                            candidate_party_id,
                            is_writein_candidate)

            candres = jointsvline(candidate_id, total, is_winner)

            if candidate_id in foundcand:
                print(f"Duplicate candidate at {linenum}:\n  {foundcand[candidate_id]} !=\n  {candline}")
            else:
                foundcand[candidate_id] = candline
                candname2id[candidate_full_name] = candidate_id
                candlist.append(candline)
                candresults.append(candres)
                if (is_writein_candidate == "1" and
                    candidate_full_name != "WRITE-IN"):
                    writein_candlist.append(candline)


        putfile("candlist-writein.tsv",
                "contest_id_eds|candidate_order|candidate_id|candidate_type|candidate_full_name|candidate_party_id|is_writein_candidate",
                writein_candlist)
        putfile("candlist-eds.tsv",
                "contest_id|candidate_order|candidate_id|candidate_type|candidate_full_name|candidate_party_id|is_writein_candidate",
                candlist)
        putfile("contresults-eds.tsv",
                "contest_id|total_precincts|processed_done|processed_started|contest_total|undervote|overvote",
                contresults)
        putfile("candresults-eds.tsv",
                "candidate_id|total|is_winner",
                candresults)
        putfile("partylist-eds.tsv",
                "party_id|party_abbr",
                partylist)




    # Convert detailed data
    # Data is in psov.tsv until the final report, then sov.tsv
    sovfile = "sov.tsv" if "sov.tsv" in zipfilenames else "psov.tsv"

    if args.verbose:
        print(f"Reading {sovfile}")
    with rzip.open(sovfile) as f:
        # Skip to the 3rd line with reporting time
        linenum = 3
        for i in range(3):
            line = f.readline().decode(SF_SOV_ENCODING).strip()

        m = re.match(r'Report generated on: \w+, (\w+ \d\d?, 20\d\d at \d\d:\d\d:\d\d \w\w)',
                     line)
        if not m:
            raise FormatError(f"sov header mismatch: {line}")

        report_time = datetime.strptime(m.group(1), "%B %d, %Y at %I:%M:%S %p")
        report_time_str = report_time.isoformat(' ')
        results_json['_reporting_time'] = report_time_str

        contest_order = 0
        contest_name = contest_name_line = ""

        total_registration = int(total_registration)

        # Enter turnout
        js_turnout = results_json['turnout'] = {
            "_id": "TURNOUT",
            #"no_voter_precincts": [],
            "precincts_reporting": int(total_precincts_reporting),
            "total_precincts": int(total_precincts),
            "eligible_voters": int(eligible_voters),
            #"reporting_time": report_time_str,
        }
        if ADD_RESULTS_VECTOR:
            js_turnout["result_stats"]= [
                {
                    "_id": "RSEli",
                    "heading": "Eligible Voters",
                    "results": [ eligible_voters, eligible_voters, eligible_voters]
                },
                {
                    "_id": "RSReg",
                    "heading": "Registered Voters",
                    "results": [total_registration, total_registration, total_registration]
                },
                {
                    "_id": "RSCst",
                    "heading": "Ballots Cast",
                    "results": [
                        int(total_precinct_ballots+total_mail_ballots),
                        int(total_precinct_ballots), int(total_mail_ballots)]
                },
                {
                    "_id": "RSRej",
                    "heading": "Ballots Challenged",
                    "results": [0,0,0]
                }
            ]
        if partyTurnout:
            js_turnout['results_summary'] = partyTurnout

        district_name_abbrs = set()

        # Load Neighborhood abbr mapping
        try:
            with TSVReader("../ems/distname-orig.tsv",
                           read_header=False) as reader:
                distabbr2code = reader.load_simple_dict(1,0)
        except Exception as ex:
            distabbr2code = {}
            print("Unable to load ../ems/distname-orig.tsv")
            print(ex)

        # line format matching:
        contest_header_pat = re2(r'\*\*\* (.+) - (.+) \((\d+)\)$')
        precinct_name_pat = re2(r'Pct (\d+)(?:/(\d+))?( MB)?$')
        district_id_pat = re2(r'^(\w+):(.+)')

        # Collected precinct consolidation
        pctcons = []        # precinct consolidation tsv
        foundpctcons = {}   # pctcons lines by ID
        contstats = []      # Table of vote stats by contest ID
        pctcontest = {}     # Collected precincts by contest
        cont_id_eds2sov = {}# Map an eds ID to sov ID


        for line in f:
            # Loop over input lines
            line = decodeline(line, SF_SOV_ENCODING)
            if (line == "" or line == "Precinct Totals" or line == contest_name_line
                or line=="District and Neighborhood Totals"):
                continue

            if contest_header_pat.match(line):
                # re.match(r'\*\*\* (.+) - (.+) \((\d+)\)$', line)
                # Start of new contest

                contest_name_line = line[4:]
                (contest_name, district_name_abbr, contest_id
                 ) = contest_header_pat.groups()
                contest_order += 1
                contest_arealines = []
                contest_totallines = []
                contest_rcvlines = []
                pctlist = []
                nv_pctlist = []     # IDs for no-voter precincts
                candidx = []        # column index for candidate heading array
                candheadings = []   # Headings for candidates, id:name
                candnames = []      # Names for candidates
                candids = []        # IDs for candidates
                rsidx = {}          # Column index by result stat ID name
                # Stats for counting precincts
                nv_precincts = ed_precincts = mv_precincts = 0

                if contest_name not in contestname2id:
                    print("Unmatched contest {contest_name} in summary")
                    contest_id_eds = ""
                    candname2id = {}
                    conteststat = {}
                else:
                    contest_id_eds = contestname2id[contest_name]
                    candname2id = contestcands[contest_id_eds]
                    conteststat = contest_status[contest_id_eds]
                    cont_id_eds2sov[contest_id_eds] = contest_id


                district_name_abbrs.add(district_name_abbr)

                vote_for = contestvotefor.get(contest_id_eds, 1)
                order = contestorder.get(contest_id_eds, f"{contest_order:03}")
                contest_type = contesttype.get(contest_id_eds,"")
                contline = jointsvline(order, contest_id, contest_id_eds,
                            contest_name, vote_for, contest_type)
                contlist.append(contline)
                omni_id = contmap_omni.get(contest_id,contest_id)
                continue

            cols = line.split('\t')
            ncols = len(cols)

            if cols[0] == "PrecinctName":
                # Header with stats and candidate columns
                indistrict = False
                if not line.startswith("PrecinctName\tReportingType\tPrecinctID\tPrecincts\tRegistration\tBallots Cast\tTurnout (%)") :
                    raise FormatError(f"sov contest header mismatch {linenum}: {line}")
                i = 4
                for name in cols[4:]:
                    # Loop over stats and candidates
                    if name in rsnamemap:
                        rsidx[rsnamemap[name]] = i
                    else:
                       candidx.append(i)
                       (partyname, writein, cand_name) = candnametrim(name)
                       candnames.append(cand_name)
                       cand_id = candname2id.get(cand_name,
                                        f'{contest_id}{len(candidx):02}')
                       candheadings.append(f'{cand_id}:{cand_name}')
                       candids.append(cand_id)
                    i += 1
                writein_col = rsidx.get('RSWri',-1)
                haswritein = writein_col > 0
                # Check for an RCV contest
                hasrcv = contest_id_eds in isrcv
                if hasrcv:
                    rs_group = "R"
                else:
                    rs_group = "C"

                #Compute result stat group
                if haswritein:
                    rs_group += "W"

                resultlist = resultlistbytype[rs_group]

                headers = ['area_id','subtotal_type'
                           ] + resultlist + candheadings;
                headerline = jointsvline(*headers)
                if args.verbose:
                    print(f"Contest({contest_id}/{contest_id_eds}:{rs_group} v={vote_for} {district_name_abbr}--{contest_name}")

                skip = False
            elif cols[0] == "DistrictName":
                # Header with stats and candidate columns
                indistrict = True
                if not line.startswith("DistrictName	ReportingType	DistrictLabel	Precincts	Registration	Ballots Cast	Turnout (%)"):
                    raise FormatError(f"sov contest header mismatch {linenum}: {line}")

            else:
                # Normal data line
                # area|ReportingType|PrecinctID

                if skip: continue
                # Set column values used below
                # cols: 0:PrecinctName|1:ReportingType|2:PrecinctID|3:Precincts|
                #   4:Registration|5:Ballots Cast|6:Turnout (%)|
                #   7:CARMEN CHU|8:PAUL BELLAR|9:WRITE-IN|10:Under Vote|11:Over Vote

                if args.zero:
                    for i in range(5,len(cols)):
                        cols[i] = "0"
                (RSReg, RSCst) = cols[4:6]
                (RSUnd, RSOvr) = cols[-2:]
                no_voter_precinct = RSReg=="0"

                # The only consolidation definition is Pct nnnn/nnnn for 2 precincts
                if cols[0] == "Grand Totals":
                    area_id = "ALL"
                    isvbm_precinct = False
                    if indistrict:
                        # Append the contest ID to the precinct ID list
                        pctids = ' '.join(sorted(pctlist))
                        if pctids in pctcontest:
                            pctcontest[pctids].append(contest_id)
                        else:
                            pctcontest[pctids] = [contest_id]

                        if rcv_rounds>1:
                            conteststat['rcv_max_votes'] = '\t'.join(
                                [str(s) for s in rcv_max_cols])
                            rcv_eliminations.reverse()
                            conteststat['rcv_eliminations'] = [
                                s.strip() for s in rcv_eliminations]

                        conteststat['results_summary'] = [
                            s.strip() for s in [headerline] +
                                contest_totallines[:ntotals]]

                        flushcontest(contest_order, contest_id, contest_name,
                                 headerline, contest_rcvlines,
                                 contest_totallines, contest_arealines)
                        continue
                elif precinct_name_pat.match(cols[0]):
                    # Area is a precinct
                    # re.match(r'Pct (\d+)(?:/(\d+))?( MB)?$', cols[0])
                    precinct_id = cols[2]
                    area_id = "PCT"+precinct_id
                    (precinct_id1, precinct_id2, vbmsuff
                     ) = precinct_name_pat.groups()
                    isvbm_precinct = vbmsuff != ""
                    if no_voter_precinct:
                        no_voter_precincts.add(contest_id)
                    precinct_name = precinct_name_orig = cols[0]
                    pctname2areaid[precinct_name_orig] = area_id
                    # Clean the name
                    precinct_name = re.sub(r'^Pct','Precinct',precinct_name)
                    pctlist.append(precinct_id1)
                    if precinct_id2:
                        pctlist.append(precinct_id2)

                    if precinct_id != precinct_id1:
                        raise FormatError(f"sov precinct id mismatch {linenum}: {line}")

                    # Check precinct consolidation
                    if precinct_id2:
                        cons_precincts = precinct_id1+" "+precinct_id2
                    else:
                        cons_precincts = precinct_id

                    newtsvlineu(foundpctcons, pctcons,
                                    "Mismatched Precinct Consolidation",
                                    precinct_id, precinct_name,
                                    boolstr(isvbm_precinct),
                                    boolstr(no_voter_precinct),
                                    cons_precincts)

                elif district_id_pat.match(cols[2]):
                    # Area is a district
                    # m = re.match(r'^(\w+):(.+)', cols[2])
                    codegroup, v = district_id_pat.groups()
                    if codegroup == "Neighborhood":
                        area_id = distabbr2code.get(v, v)
                    else:
                        area_id = distcodemap[codegroup]+v

                    # Skip countywide (duplicate of totals
                    if no_voter_precinct or area_id in countywide_districts:
                        continue

                else:
                    # Unmatched Area
                    raise FormatError(f"sov area mismatch {linenum}: {line}")


                # Map the subtotal_type
                if cols[1] == 'Election Day':
                    subtotal_type = 'ED'
                    if area_id != "ALL":
                        checkDuplicate(pctturnout_reg, precinct_name_orig, RSReg,
                                    "Registration")
                        checkDuplicate(pctturnout_ed, precinct_name_orig, RSCst,
                                       "Election Day Turnout")
                        if isvbm_precinct or no_voter_precinct:
                            continue    # Skip ED reporting for VBM-only precincts
                        ed_precincts += 1
                elif cols[1] == 'VBM':
                    subtotal_type = 'MV'
                    if area_id != "ALL":
                        checkDuplicate(pctturnout_reg, precinct_name_orig, RSReg,
                                        "Registration")
                        checkDuplicate(pctturnout_mv, precinct_name_orig, RSCst,
                                        "Vote-By-Mail Turnout")
                        if no_voter_precinct:
                            nv_precincts += 1
                            nv_pctlist.append(area_id)
                        else:
                                mv_precincts += 1
                        if no_voter_precinct and not args.withzero:
                            # Skip including zero precincts
                            continue
                    # End not all precincts
                else:
                    subtotal_type = 'TO'

                # Compute total votes
                total_votes = sum(map(int,cols[7:-2]))
                RSTot = int(total_votes)

                # Compute ignored
                RSExh = "0"
                RSCst_orig = RSCst

                total_ballots = int((int(RSTot)+int(RSUnd)+int(RSOvr))/vote_for)
                if False:
                    # Flaw in sov - counts are from card 0 precinct sum
                    RSRej = int(total_ballots - int(RSCst))
                    RSCst = int(total_ballots)
                else:
                    RSRej = int(int(RSCst)-total_ballots)

                if hasrcv:
                    stats = [area_id, subtotal_type, RSReg, RSCst,
                            RSRej, RSOvr, RSUnd, RSExh, RSTot]
                else:
                    stats = [area_id, subtotal_type, RSReg, RSCst,
                            RSRej, RSOvr, RSUnd, RSTot]
                if haswritein:
                    stats.append(cols[writein_col]) # First write-in
                    cand_start_col = len(stats)
                    stats.extend(cols[7:writein_col]) # Regular candidates
                    stats.extend(cols[writein_col+1:-2]) # Named write-in
                else:
                    cand_start_col = len(stats)
                    stats.extend(cols[7:-2])
                outline = jointsvline(*stats)
                if area_id == "ALL":
                    if subtotal_type == 'TO':
                        # Put grand totals first
                        if args.verbose and RSRej!="0":
                            print(
    f"RSRej={RSRej} Cst={RSCst_orig} T={RSTot}:{RSUnd}:{RSOvr} for {contest_id}:{contest_name}")

                        contest_totallines.insert(0, outline)

                        # Save/Check total [s for results summary
                        if hasrcv:
                            # stats:subtotal_type, RSReg, RSCst, RSRej,
                            contest_rcvlines, final_cols = loadRCVData(
                                rzip, contest_name, candnames, stats[1:5])
                            candvotes = final_cols[cand_start_col:]
                        else:
                            contest_rcvlines = []
                            candvotes = stats[cand_start_col:]
                        rcv_rounds = len(contest_rcvlines)
                        winning_status = {}
                        cand_success = {}   # True/False for winning_status

                        if total_votes:
                            approval_required = approval_required_by_omni_id.get(
                                omni_id,'')
                            if approval_required:
                                # Set pass_fail status
                                if approval_required == 'Majority':
                                    votes_required = (total_votes//2) + 1
                                elif approval_fraction_pat.match(approval_required):
                                    (num, denom) = map(int, approval_fraction_pat.groups())
                                    votes_required = (total_votes*num+denom-1)//denom
                                elif approval_percent_pat.match(approval_required):
                                    votes_required = ((total_votes*
                                         approval_percent_pat.group(1)+00)//100)
                                else:
                                    votes_required = 0
                                if votes_required:
                                    if int(candvotes[0]) >= votes_required:
                                        conteststat['success'] = True
                                        win_id = candids[0]
                                        lose_id = candids[1]
                                    else:
                                        conteststat['success'] = False
                                        win_id = candids[1]
                                        lose_id = candids[0]
                                    winning_status[win_id] = 'W'
                                    winning_status[lose_id] = 'N'
                                    cand_success[win_id] = True
                                    cand_success[lose_id] = False
                            else:
                                candvotes_by_id = zip(candids, candvotes)
                                # Compute winners
                                ranked_candvotes = sorted(candvotes_by_id,
                                        key=lambda x: int(x[1]) if x[1] != '' else 0,
                                        reverse=True)
                                ncands = len(ranked_candvotes)
                                # TODO: Conditional runoff
                                # TODO: Handle RCV with more than one elected
                                nwinners = 1 if hasrcv else vote_for
                                has_runoff = runoff_by_contid.get(contest_id, None)
                                conditional_runoff_limit = 0
                                if has_runoff:
                                    runoff_type = has_runoff['runoff_type']
                                    conteststat['to_runoff'] = True
                                    if runoff_type == 'non_majority':
                                        conditional_runoff_limit = (
                                            (total_votes//2) + 1)
                                runoff_status = True if has_runoff else ''
                                if has_runoff:
                                    nwinners += 1

                                last_winner = None
                                last_v = total_votes
                                #print(f"ranked_candvotes {contest_id}:{contest_name}",
                                    #ranked_candvotes)
                                for c,v in ranked_candvotes:
                                    if nwinners > 0:
                                        # The next wins
                                        # TODO: conditional runoff with vote_for>1
                                        if conditional_runoff_limit:
                                            if int(v)>conditional_runoff_limit:
                                                nwinners -= 1
                                                runoff_status = False
                                                if nwinners < 2:
                                                    conteststat['to_runoff'] = False
                                        winning_status[c] = 'R' if runoff_status else 'W'
                                        nwinners -= 1
                                        last_winner = c
                                        last_v = v
                                        cand_success[c] = True
                                    elif v == last_v:
                                        # Tie for winner
                                        winning_status[last_winner] = 'T'
                                        winning_status[c] = 'T'
                                        cand_success[c] = True
                                    elif hasrcv and not v:
                                        winning_status[c] = 'E'
                                        cand_success[c] = False
                                    else:
                                        winning_status[c] = 'N'
                                        cand_success[c] = False

                        # Check precinct counts
                        total_precincts = precinct_count[contest_id_eds]
                        if args.verbose:
                            print(f"  precincts ed/mv/nv={ed_precincts}/{mv_precincts}/{nv_precincts} of {total_precincts}")

                        # Append json file output
                        conteststat['_id'] = contest_id
                        #conteststat['reporting_time'] = report_time_str
                        #conteststat['no_voter_precincts'] = nv_pctlist
                        conteststat['rcv_rounds'] = rcv_rounds

                        if rcv_rounds>1:
                            # Compute
                            n = rcv_rounds-1
                            rcv_max_cols = list(final_cols)
                            rcv_eliminations = []
                            for line in contest_rcvlines[1:]:
                                elim = ''
                                for i,v in enumerate(line.strip('\n').split('\t')):
                                    if rcv_max_cols[i]== '' and v !='':
                                        rcv_max_cols[i] = v
                                        ic = i-cand_start_col
                                        if ic<0:
                                            continue
                                        elim += f"\t{candids[ic]}:{candnames[ic]}"
                                rcv_eliminations.append(elim)
                            rcv_max_cols[0]='RCVMAX'


                        # Split the totallines into a matrix
                        totals = [ line.rstrip().split(separator)
                                  for line in contest_totallines]
                        ntotals = len(totals)
                        if ADD_RESULTS_VECTOR:
                            i = 2 # Starting index for result stats
                            for rsid in resultlist:
                                conteststat['result_stats'].append({
                                    "_id": rsid,
                                    "heading": VOTING_STATS[rsid],
                                    "results":[int(totals[j][i]) for j in range(ntotals)]
                                    })
                                i += 1
                        k = 0
                        cont_winning_status = defaultdict(str)
                        for candid in candids:
                            status = winning_status_names[winning_status.get(candid,'')]
                            if status != ('rcv_eliminated' if rcv_rounds
                                          else 'not_winning'):
                                cont_winning_status[status]+=f"\t{candid}:{candnames[k]}"
                            if ADD_CHOICES:
                                choice_js = {
                                "_id": candid,
                                "heading": candnames[k],
                                "success": cand_success.get(candid,''),
                                }
                                if ADD_RESULTS_VECTOR:
                                    choice_js["winning_status"] = status
                                    choice_js["results"] =[totals[j][i] for j in range(ntotals)]
                                conteststat['choices'].append(choice_js)
                                i += 1
                            k += 1

                        conteststat['winning_status']= {
                            k:v.strip() for (k,v) in cont_winning_status.items()}
                        results_contests.append(conteststat)

                        # Form a line with summary stats by ID
                        newtsvline(contstats, contest_id,
                                   RSReg, RSCst, RSRej, total_ballots,
                                   RSOvr, RSUnd, RSTot, vote_for,
                                   contest_name)

                    # End all precincts total
                    else:
                        # totals but not all precinct
                        # Add ED and MV
                        contest_totallines.append(outline)
                # End all precincts
                else:
                    # Not all precincts
                    # Append area lines separately
                    contest_arealines.append(outline)

            # End normal data line
        # End Loop over input lines

        # Put the json contest status
        with open(f"{OUT_DIR}/results.json",'w') as outfile:
            json.dump(results_json, outfile, **json_dump_args)

        # Put the precinct consolidation file
        putfile("pctcons-sov.tsv",
                "cons_precinct_id|cons_precinct_name|is_vbm|no_voters|precinct_ids",
                pctcons)

        # Put the contest vote stats file
        putfile("contstats-sov.tsv",
                "contest_id|registration|ballots_cast|ballots_uncounted|computed_ballots|"+
                "overvotes|undervotes|totalvotes|vote_for|contest_name",
                contstats)

        # Put the extracted contest list
        putfile("contlist-sov.tsv",
                "contest_order|contest_id_sov|contest_id_eds|contest_full_name|vote_for|contest_type",
                contlist)
        # Put the precinct_list to contest file
        pctcontest_lines = []
        for precinct_ids in pctcontest.keys():
            newtsvline(pctcontest_lines, precinct_ids,
                    ' '.join(sorted(pctcontest[precinct_ids])))
        putfile("pctcont-sov.tsv",
                "precinct_ids|contest_ids",
                pctcontest_lines)
    # End reading sov.tsv

    header = "precinct_id\tname\tlist_order\ttype_1\ttype_2\ttype_3\ttype_4\ttype_5"\
        "\ttype_6\tcmax_category_order\ted_turnout\tev_turnout\tMAIL_turnout"\
        "\ttype4_turnout\ttype5_turnout\ttype6_turnout"\
        "\tturnout"    # Finalize

    if args.verbose:
        print("Reading turnout.txt")
    with rzip.open("turnout.txt") as f:
        linenum = 1
        pctturnout = []
        ed_total = reg_total = mv_total = 0
        line = f.readline().decode(SF_ENCODING).strip()
        if line!=header:
            raise FormatError(f"Header mismatch:\n'{line}' should be\n'{header}")

        for line in f:
            line = decodeline(line)

            (precinct_id, name, list_order, type_1, type_2, type_3, type_4, type_5,
            type_6, cmax_category_order, ed_turnout, ev_turnout, MAIL_turnout,
            type4_turnout, type5_turnout, type6_turnout, turnout
            ) = line.split('\t')

            # Note fields are wrong-- ev_turnout is vbm

            reg = pctturnout_reg[name]
            checkDuplicate(pctturnout_ed, name, ed_turnout,
                                        "Election Day Turnout")
            checkDuplicate(pctturnout_mv, name, ev_turnout,
                                        "Vote-By-Mail Turnout")

            newtsvline(pctturnout, pctname2areaid[name], reg,
                       turnout, ed_turnout, ev_turnout)

            reg_total += int(reg)
            ed_total += int(ed_turnout)
            mv_total += int(ev_turnout)


        # End Loop over input lines
        newtsvline(pctturnout, "ALL", reg_total,
                   ed_total+mv_total, ed_total, mv_total)

        candlist_sov = []
        for l in candlist:
            contest_id_eds = l.split(separator,1)[0]
            if contest_id_eds not in cont_id_eds2sov:
                print(f"Unmatched EDS contest {l}")
                continue
            candlist_sov.append(cont_id_eds2sov[contest_id_eds]+
                                separator+l)
        putfile("candlist-sov.tsv",
                "contest_id|contest_id_eds|candidate_order|candidate_id|candidate_type|candidate_full_name|candidate_party_id|is_writein_candidate",
                candlist_sov)

        putfile("pctturnout.tsv",
                "area_id|registration|total_ballots|ed_ballots|mv_ballots",
                pctturnout)

    # End reading zip file








