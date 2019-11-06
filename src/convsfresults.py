#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2018  Carl Hage
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
Converts 2019 Dominion xlsx SOV format to extract result files.
"""

# Library References
import json
import logging
import os
import os.path
import re
import argparse
import struct
import string
import operator, functools

# Local file imports
from tsvio import TSVReader
from re2 import re2

# Library imports
from datetime import datetime
from collections import OrderedDict, namedtuple
from typing import List, Pattern, Match, Dict
from zipfile import ZipFile

DESCRIPTION = """\
Converts election results data from downloaded from
sfgov.org Election Results - Detailed Reports

Reads the following files:
  * resultdata-raw.zip/psov.psv - Downloaded SF results
  * [TODO] ../election.json - Election definition data
  * [TODO] config-odc.yaml - Configuration file with conversion options

Creates the following files:
  * contest-status.json
  * results-{contest_id}.tsv
  * A set of intermediate files extracted from the resultdata-raw

"""
#TODO:
# -

VERSION='0.0.1'     # Program version

SF_ENCODING = 'ISO-8859-1'
SF_SOV_ENCODING ='UTF-8'
SF_HTML_ENCODING = 'UTF-8'

OUT_DIR = "../out-orr/resultdata"

SOV_FILE = "psov"

DEFAULT_JSON_DUMP_ARGS = dict(sort_keys=True, separators=(',\n',':'), ensure_ascii=False)
PP_JSON_DUMP_ARGS = dict(sort_keys=True, indent=4, ensure_ascii=False)

approval_fraction_pat = re2(r'^(\d+)/(\d+)$')
approval_percent_pat = re2(r'^(\d+)%$')

# Result Stats by type
resultlistbytype = {}
for line in """\
CW=RSReg RSCst RSRej RSOvr RSUnd RSTot RSWri
RW=RSReg RSCst RSRej RSOvr RSUnd RSExh RSTot RSWri
T=RSReg RSCst RSRej
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



args = parse_args()

if args.dirsuffix:
    OUT_DIR = OUT_DIR+'-'+args.dirsuffix
elif args.zero:
    OUT_DIR = OUT_DIR+'-zero'

if not os.path.exists(OUT_DIR):
    os.makedirs(OUT_DIR)

json_dump_args = PP_JSON_DUMP_ARGS if args.pretty else DEFAULT_JSON_DUMP_ARGS

separator = "|" if args.pipe else "\t"

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

def loadEligible()->str:
    """
    Reads the ../vr/county.tsv file to locate eligible voters by county.
    Returns the count as a string, a numbmer or null.
    """
    try:
        with TSVReader("../vr/county.tsv") as reader:
            for l in reader.readlines():
                if l[0] == "San Francisco":
                    return l[1]
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
    filename = string.capwords(contest_name.lower())
    # Pattern match the file names
    if (re2c.sub2ft(filename, r'.*Supervisor\D+(\d+)$', 'RcvShortReport_D{0}.psv') or
        re2c.sub2ft(filename, r'^.*(District Attorney).*$', 'RcvShortReport_DA.psv') or
        re2c.sub2ft(filename, r'^.*(Mayor|Assessor|Defender|Attorney|Sheriff|Treasurer).*$', 'RcvShortReport_{0}.psv')):
        filename = re2c.string
    else:
        raise FormatError(f"Unmatched RCV contest name {filename}")


    rcvtable = [ [] for i in range(len(candnames) + 5) ]
    rcvlines = []
    if filename not in zipfilenames:
        print(f"RCV file {filename} not found")
        return rcvlines, []
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
candbyname = {}

# Output file struct
contest_status_json = []

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

def load_json_table(
    rzip,           # Opened CVR_Export zip file
    filename:str,   # JSON file to load
    version:str,    # Expected version string
    attrs:dict,     # Expected attributes
    keyattr:str,    # Attribute for data key
    altdict:dict=None,   # Secondary dict or none
    altkeyattr:str=None, # Secondary dict index attriute
    )->Dict[str,object]: # Returns indexed object
    """
    Extract and load the json data to a parsed object
    """
    with rzip.open(filename) as f:
        objtype = namedtuple(filename[:-5],attrs.keys())
        j = json.load(f)
        js_version = j['Version'];
        if js_version!=version:
            print(f"Warning: {filename} version is {js_version} not {version}")
        d = {}
        for i in j['List']:
            for a,t in attrs.items():
                v = i.get(a)
                if v==None:
                    v = 0 if t==int else ""
                i[a] = t(v)
            r = objtype(**i)
            d[i[keyattr]]=r
            if altdict!=None:
                altdict[i[altkeyattr]]=r
    return d




# Extract Manifest data from the CVR json
ContestManifest_attrs= {
    "Description":str,
    "Id":str,
    "ExternalId":str,
    "VoteFor":int,
    "NumOfRanks":int
    }

CandidateManifest_attrs= {
    "Description":str,
    "Id":str,
    "ExternalId":str,
    "ContestId":str,
    "Type":str,
    }

with ZipFile("CVR_Export.zip") as rzip:
    ContestManifest_by_Id = {}
    ContestManifest = load_json_table(rzip,"ContestManifest.json","5.2.18.2",
                           ContestManifest_attrs, "Description",
                           ContestManifest_by_Id, "Id")
    CandidateManifest_by_ContestId = {}
    CandidateManifest_by_Id = load_json_table(rzip,"CandidateManifest.json","5.2.18.2",
                           CandidateManifest_attrs, "Id");

    for r in CandidateManifest_by_Id.values():
        if not r.ContestId in CandidateManifest_by_ContestId:
            CandidateManifest_by_ContestId[r.ContestId] = {}
        CandidateManifest_by_ContestId[r.ContestId][r.Description] = r


# Process the downloaded SF results stored in resultdata-raw.zip
with ZipFile("resultdata-raw.zip") as rzip:
    rfiles = rzip.infolist()

    # Create a set with the zip file names
    zipfilenames = set()
    for info in rfiles:
        zipfilenames.add(info.filename)

    # Read turnout details

    # Read the SOV results
    if "sov.psv" in zipfilenames:
        sovfile = "sov.psv"
        sep = '|'
    elif "sov.tsv" in zipfilenames:
        sovfile = "sov.tsv"
        sep = '\t'
    elif "psov.psv" in zipfilenames:
        sovfile = "psov.psv"
        sep = '|'
    elif "psov.tsv" in zipfilenames:
        sovfile = "psov.tsv"
        sep = '\t'
    else:
        raise FormatError(f"Unmatched SOV XLSX file")

    if args.verbose:
        print(f"Reading {sovfile}")
    with rzip.open(sovfile) as f:
        linenum = 0
        candlist = []
        contlist = []
        pctlist = []
        tallylist = []
        contest_name = ''
        in_turnout = False
        subtotal_col = -1
        pctname2areaid = {} # Map original precinct ID to area ID

        # Patterns for line type
        page_header_pat = re2(r'\f?Page: (\d+) of \d+[\|\t]+(20\d\d-\d\d-\d\d[ :\d]+)$')
        contest_header_pat=re2(r'(.*) \(Vote for +(\d+)\)$')
        precinct_name_pat = re2(r'PCT (\d+)(?:/(\d+))?( MB)?$')
        skip_area_pat = re2(r'^(Cumulative|San Francisco - Total|City and County - Total)$')
        TURNOUT_LINE_SUFFIX = '% Turnout'

        # Collected precinct consolidation
        pctcons = []        # precinct consolidation tsv
        foundpctcons = {}   # pctcons lines by ID
        contstats = []      # Table of vote stats by contest ID
        pctcontest = {}     # Collected precincts by contest
        cont_id_eds2sov = {}# Map an eds ID to sov ID
        pctturnout = []
        precinct_count = {}

        # Note:Not in current files
        district_id_pat = re2(r'^(\w+):(.+)')

        # Used to make ID/sequence
        contest_order = 0
        skip_lines = 0;

        for line in f:
            linenum += 1
            line = decodeline(line, SF_SOV_ENCODING)
            if line == "" or line.endswith("Statement of the Vote"):
                # Ignore blank lines
                continue

            if skip_lines:
                skip_lines -= 1
                continue

            # Check for a page header
            if page_header_pat.match(line):
                page, report_time_str = page_header_pat.groups()
                if args.verbose:
                    print(f"Page {page}: {report_time_str}")
                in_turnout = False
                contest_name=''
                continue

            line = re.sub(r'Registered ?␤Voters','Registered Voters',line)

            if contest_name=='':
                if line.endswith(TURNOUT_LINE_SUFFIX):
                    in_turnout = True
                    contest_id = contest_name = 'TURNOUT'
                elif contest_header_pat.match(line):
                    contest_name, vote_for = contest_header_pat.groups()
                    contest_order += 1
                    vote_for = int(vote_for)
                    if contest_name not in ContestManifest:
                        print(f"Unmatched contest {contest_name}")
                        contest_id = 'X'+str(contest_order).zfill(3)
                        candbyname = {}
                        max_ranked = 0
                        contest_id_ext = 0
                    else:
                        contest = ContestManifest[contest_name]
                        contest_id = contest.Id
                        contest_id_ext = contest.ExternalId
                        candbyname = CandidateManifest_by_ContestId.get(contest_id,{})
                        max_ranked = contest.NumOfRanks
                        if contest.VoteFor != vote_for:
                            print(f"Mismatched VoteFor in {contest_name}: {contest.VoteFor} != {vote_for}")
                    if max_ranked > 0:
                        isrcv.add(contest_id)
                    contline = jointsvline(contest_order, contest_id,
                                           contest_id_ext,
                                contest_name, vote_for, max_ranked)
                    contlist.append(contline)
                else:
                    raise FormatError(f"sov contest name mismatch {linenum}:{line}")


                contest_arealines = []
                contest_totallines = []
                contest_rcvlines = []
                pctlist = []
                nv_pctlist = []     # IDs for no-voter precincts
                candidx = []        # column index for candidate heading array
                candheadings = []   # Headings for candidates, id:name
                candnames = []      # Names for candidates
                cand_order = 0      # Candidate sequence
                candids = []        # IDs for candidates
                rsidx = {}          # Column index by result stat ID name
                rs_group = ''       # Set of Result Stats
                contest_id_eds = ""
                processed_done = total_precincts = 0
                # Stats for counting precincts
                nv_precincts = ed_precincts = mv_precincts = 0
                if not in_turnout:
                    continue

            cols = [s.rstrip(' ␤') for s in line.split(sep)]
            ncols = len(cols)

            if not rs_group:
                # Next line should be headings
                # Followed by 2 area lines to be skipped
                skip_lines = 2
                expected_cols = ncols
                if (cols[0] != 'Precinct' or
                    cols[1] != 'Registered Voters'):
                    raise FormatError(f"sov column header mismatch {linenum}:{line}")

                if in_turnout:
                    if (cols[2] or cols[3] != 'Cards Cast' or cols[4] or
                        cols[5] != 'Voters Cast' or
                        cols[6] != '% Turnout'):
                        raise FormatError(f"sov turnout column header mismatch {linenum}:{line}")
                    rs_group = 'T'
                    hasrcv = haswritein = 0
                else:
                    total_col = ncols - 1
                    writein_col = 0
                    if (cols[2] != 'Undervotes' or
                        cols[4] != 'Overvotes' or
                        cols[5] != 'Precinct' or
                        cols[total_col] != 'Total Votes'):
                        raise FormatError(f"sov column header mismatch {linenum}:{cols}")

                    for i in range(6, total_col, ):
                        name = cols[i]
                        if name == '':
                            continue
                        if name == 'Write-in':
                            heading = 'RSWri'
                            writein_col = i
                        else:
                            candidate_full_name = name
                            cand_order += 1
                            candidx.append(i)
                            candidate_order = len(candidx)
                            candnames.append(name)
                            candidate_party_id = ""
                            if name not in candbyname:
                                print(f"Can't match {name} in {contest_id}:{contest_name}");
                                cand_id = f'{contest_id}{candidate_order:02}'
                                candidate_type = ""
                                is_writein_candidate = False
                            else:
                                candidate = candbyname[name]
                                cand_id = candidate.Id
                                candidate_type = candidate.Type
                                is_writein_candidate = re.search(
                                    r'Writein', candidate_type, flags=re.I) != None

                            candheadings.append(f'{cand_id}:{name}')
                            candids.append(cand_id)

                            candline = jointsvline(contest_id,
                                            candidate_order, cand_id,
                                            candidate_type,
                                            candidate_full_name,
                                            candidate_party_id,
                                            boolstr(is_writein_candidate))
                            candlist.append(candline)
                        # End loop over candidate names
                    haswritein = writein_col > 0
                    # Check for an RCV contest
                    hasrcv = contest_id in isrcv
                    if hasrcv:
                        rs_group = "R"
                    else:
                        rs_group = "C"

                    #Compute result stat group
                    if haswritein:
                        rs_group += "W"

                    if args.verbose:
                        print(f"Contest({contest_id}:{rs_group} v={vote_for} {contest_name}")

                resultlist = resultlistbytype[rs_group]
                headers = ['area_id','subtotal_type'
                           ] + resultlist + candheadings;
                headerline = jointsvline(*headers)

            else:
                # Normal data line
                # Column of data
                if skip_area_pat.match(cols[0]):
                    # Superflous totals
                    continue

                if expected_cols != ncols:
                    raise FormatError(f"Mismatched Column count {ncols}!={expected_cols} {linenum}:{line}")

                if args.zero:
                    for i in range(2,len(cols)):
                        cols[i] = "0"
                if in_turnout:
                    (RSReg, RSCards, RSCst) = [int(float(cols[i])) for i in [1,3,5]]
                    RSTrn = cols[6]
                    RSRej = 0 # Not available
                    if RSTrn == "N/A":
                        RSTrn = "0.0";
                else:
                    (RSReg, RSUnd, RSOvr) = [int(float(cols[i])) for i in [1,2,4]]
                    total_votes = RSTot = int(float(cols[total_col]))
                    RSCst = total_ballots = int(RSOvr)+int((int(RSTot)+int(RSUnd))/vote_for)
                    # RSRej not available
                    RSRej = RSExh = 0

                no_voter_precinct = RSReg=="0" and not args.withzero

                if cols[0] == "Cumulative - Total":
                    area_id = "ALLPCTS"
                    isvbm_precinct = False
                elif precinct_name_pat.match(cols[0]):
                    # Area is a precinct
                    # re.match(r'PCT (\d+)(?:/(\d+))?( MB)?$', cols[0])
                    (precinct_id, precinct_id2, vbmsuff
                     ) = precinct_name_pat.groups()
                    area_id = "PCT"+precinct_id
                    isvbm_precinct = vbmsuff != ""
                    if no_voter_precinct:
                        no_voter_precincts.add(contest_id)
                    precinct_name = precinct_name_orig = cols[0]
                    pctname2areaid[precinct_name_orig] = area_id
                    # Clean the name
                    precinct_name = re.sub(r'^PCT','Precinct',precinct_name)
                    pctlist.append(precinct_id)
                    if precinct_id2:
                        pctlist.append(precinct_id2)

                    # Check precinct consolidation
                    if precinct_id2:
                        cons_precincts = precinct_id+" "+precinct_id2
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
                subtotal_type = 'TO'
                if subtotal_col >= 0:
                    if cols[subtotal_col] == 'Election Day':
                        subtotal_type = 'ED'
                        if area_id != "ALLPCTS":
                            checkDuplicate(pctturnout_reg, precinct_name_orig, RSReg,
                                        "Registration")
                            checkDuplicate(pctturnout_ed, precinct_name_orig, RSCst,
                                        "Election Day Turnout")
                            if isvbm_precinct or no_voter_precinct:
                                continue    # Skip ED reporting for VBM-only precincts
                            ed_precincts += 1
                    elif cols[subtotal_col] == 'VBM':
                        subtotal_type = 'MV'
                        if area_id != "ALLPCTS":
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
                        ## End not all precincts
                if in_turnout:
                    newtsvline(pctturnout, area_id, RSReg,
                            RSCst)
                    continue
                else:
                    if hasrcv:
                        stats = [area_id, subtotal_type, RSReg, RSCst,
                                RSRej, RSOvr, RSUnd, RSExh, RSTot]
                    else:
                        stats = [area_id, subtotal_type, RSReg, RSCst,
                                RSRej, RSOvr, RSUnd, RSTot]
                    if haswritein:
                        stats.append(cols[writein_col]) # First write-in
                    cand_start_col = len(stats)
                    stats.extend([cols[i] for i in candidx])

                    if area_id.startswith("PCT") and (
                        subtotal_col <0 or subtotal_type == 'ED' or
                        isvbm_precinct):
                        total_precincts += 1
                        if RSTot:
                            processed_done += 1

                outline = jointsvline(*stats)
                if area_id == "ALLPCTS":
                    if subtotal_type == 'TO':
                        # Put grand totals first

                        contest_totallines.insert(0, outline)

                        # Save/Check total [s for results summary
                        candvotes = stats[cand_start_col:]
                        if hasrcv:
                            # stats:subtotal_type, RSReg, RSCst, RSRej,
                            contest_rcvlines, final_cols = loadRCVData(
                                rzip, contest_name, candnames, stats[1:5])
                            if final_cols:
                                candvotes = final_cols[cand_start_col:]
                            else:
                               hasrcv = False
                        else:
                            contest_rcvlines = []
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
                                        winning_status[c] = 'R' if has_runoff else 'W'
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
                        #total_precincts = precinct_count[contest_id]
                        #if args.verbose:
                            #print(f"  precincts ed/mv/nv={ed_precincts}/{mv_precincts}/{nv_precincts} of {total_precincts}")

                        # Append json file output
                        conteststat = {
                            'choices': [],
                            'precincts_reporting': int(processed_done),
                            'reporting_time': '',
                            'result_stats': [],
                            'total_precincts': int(total_precincts)
                            }
                        conteststat['_id'] = contest_id
                        conteststat['reporting_time'] = report_time_str
                        conteststat['no_voter_precincts'] = nv_pctlist
                        conteststat['rcv_rounds'] = rcv_rounds
                        # Split the totallines into a matrix
                        totals = [ line.rstrip().split(separator)
                                  for line in contest_totallines]
                        ntotals = len(totals)
                        i = 2 # Starting index for result stats
                        for rsid in resultlist:
                            conteststat['result_stats'].append({
                                "_id": rsid,
                                "heading": VOTING_STATS[rsid],
                                "results":[totals[j][i] for j in range(ntotals)]
                                })
                            i += 1
                        k = 0
                        for candid in candids:
                            conteststat['choices'].append({
                                "_id": candid,
                                "ballot_title": candnames[k],
                                "winning_status": winning_status_names[
                                    winning_status.get(candid,'')],
                                "success": cand_success.get(candid,''),
                                "results":[totals[j][i] for j in range(ntotals)]
                                })
                            i += 1
                            k += 1

                        contest_status_json.append(conteststat)

                        # Form a line with summary stats by ID
                        newtsvline(contstats, contest_id,
                                   RSReg, RSCst, RSRej, total_ballots,
                                   RSOvr, RSUnd, RSTot, vote_for,
                                   contest_name)

                        # Append the contest ID to the precinct ID list
                        pctids = ' '.join(sorted(pctlist))
                        if pctids in pctcontest:
                            pctcontest[pctids].append(contest_id)
                        else:
                            pctcontest[pctids] = [contest_id]

                        flushcontest(contest_order, contest_id, contest_name,
                                 headerline, contest_rcvlines,
                                 contest_totallines, contest_arealines)
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
        with open(f"{OUT_DIR}/contest-status.json",'w') as outfile:
            json.dump(contest_status_json, outfile, **json_dump_args)

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
                "contest_order|contest_id_sov|contest_id_ext|contest_full_name|vote_for|max_ranked",
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

    putfile("candlist-sov.tsv",
            "contest_id|candidate_order|candidate_id|candidate_type|candidate_full_name|candidate_party_id|is_writein_candidate",
            candlist)

    putfile("pctturnout.tsv",
            "area_id|registration|total_ballots|ed_ballots|mv_ballots",
            pctturnout)

    # End reading zip file








