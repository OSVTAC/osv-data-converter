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
from shautil import load_sha_file, load_sha_filename, SHA_FILE_NAME
from tsvio import TSVReader
from re2 import re2
from config import Config, config_idlist
from translations import Translator

# Library imports
from datetime import datetime
from collections import OrderedDict, namedtuple, defaultdict

from typing import List, Pattern, Match, Dict, Set, TextIO, Union
from zipfile import ZipFile

DESCRIPTION = """\
Converts election results data from downloaded from
sfgov.org Election Results - Detailed Reports

Reads the following files:
  * resultdata-raw.zip/psov.psv - Downloaded SF results
  * [TODO] ../election.json - Election definition data
  * [TODO] config-results.yaml - Configuration file with conversion options

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

_log = logging.getLogger(__name__)

VERSION='0.0.2'     # Program version

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

SOV_FILE = "psov"

# have_EDMV = True (Set by !config.all_mail_election)
have_RSCst = True

check_duplicate_turnout = False

DEFAULT_JSON_DUMP_ARGS = dict(sort_keys=True, separators=(',\n',':'), ensure_ascii=False)
PP_JSON_DUMP_ARGS = dict(sort_keys=True, indent=4, ensure_ascii=False)

approval_fraction_pat = re2(r'^(\d+)/(\d+)$')
approval_percent_pat = re2(r'^(\d+)%$')

CONFIG_FILE = "config-results.yaml"
config_attrs = dict(
    card_turnout_contests=config_idlist,
    all_mail_election=bool,
    grand_totals_wrong=bool,
    )

# Collect district names for map
#df = open("distabbr.txt",'w')

# Result Stats by type
resultlistbytype = {}
for line in """\
EMCW=RSReg RSCst RSRej RSOvr RSUnd RSTot RSWri
EMRW=RSReg RSCst RSRej RSOvr RSUnd RSExh RSTot RSWri
EMT=RSReg RSCst RSRej
EMC=RSReg RSCst RSRej RSOvr RSUnd RSTot""".split('\n'):
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
    ('RSBla', 'Blank Ballots'),     # Ballots with no choices in this contest
    ('RSUnd', 'Undervotes'),        # Blank votes or additional votes not made
    ('RSOvr', 'Overvotes'),         # Possible votes rejected by overvoting
    ('RSExh', 'Exhausted Ballots')  # All RCV choices were eliminated (RCV only)
    ])
# These district subtotals are meaningless

COUNTY_DISTRICT_ID = 'COSF'
countywide_districts = {COUNTY_DISTRICT_ID,'SEN11'}
vgnamemap = {'Total':'TO',
             'Election Day':'ED',
             'Vote by Mail':'MV',
             'VBM':'MV'}

rsnamemap = {'Registration':'RSReg',
             'Ballots Cast':'RSCst',
             'Turnout (%)':'RSTrn',     # Unused turnout percent
             'WRITE-IN':'RSWri',
             'Under Vote':'RSUnd',
             'Over Vote':'RSOvr' }

distcodemap = {
    'CONGRESSIONAL DISTRICT 12':'CONG12',
    'CONGRESSIONAL DISTRICT 13':'CONG13',
    'CONGRESSIONAL DISTRICT 14':'CONG14',
    'CONG 12':'CONG12',
    'CONG 13':'CONG13',
    'CONG 14':'CONG14',
    'ASSEMBLY DISTRICT 17':'ASSM17',
    'ASSEMBLY DISTRICT 19':'ASSM19',
    'SUPERVISORIAL DISTRICT 1':'SUPV1',
    'SUPERVISORIAL DISTRICT 2':'SUPV2',
    'SUPERVISORIAL DISTRICT 3':'SUPV3',
    'SUPERVISORIAL DISTRICT 4':'SUPV4',
    'SUPERVISORIAL DISTRICT 5':'SUPV5',
    'SUPERVISORIAL DISTRICT 6':'SUPV6',
    'SUPERVISORIAL DISTRICT 7':'SUPV7',
    'SUPERVISORIAL DISTRICT 8':'SUPV8',
    'SUPERVISORIAL DISTRICT 9':'SUPV9',
    'SUPERVISORIAL DISTRICT 10':'SUPV10',
    'SUPERVISORIAL DISTRICT 11':'SUPV11',
    'ASMBLY 17':'ASSM17',
    'ASMBLY 19':'ASSM19',
    'SUP DIST 1':'SUPV1',
    'SUP DIST 2':'SUPV2',
    'SUP DIST 3':'SUPV3',
    'SUP DIST 4':'SUPV4',
    'SUP DIST 5':'SUPV5',
    'SUP DIST 6':'SUPV6',
    'SUP DIST 7':'SUPV7',
    'SUP DIST 8':'SUPV8',
    'SUP DIST 9':'SUPV9',
    'SUP DIST 10':'SUPV10',
    'SUP DIST 11':'SUPV11',
    'BAYVIEW/HUNTERS POINT':'NEIG1',
    'CHINATOWN':'NEIG2',
    'CIVIC CENTER/DOWNTOWN':'NEIG3',
    'DIAMOND HEIGHTS':'NEIG4',
    'EXCELSIOR (OUTER MISSION)':'NEIG5',
    'HAIGHT ASHBURY':'NEIG6',
    'INGLESIDE':'NEIG7',
    'INNER SUNSET':'NEIG8',
    'LAKE MERCED':'NEIG9',
    'LAUREL HEIGHTS/ANZA VISTA':'NEIG10',
    'MARINA/PACIFIC HEIGHTS':'NEIG11',
    'MISSION':'NEIG12',
    'NOE VALLEY':'NEIG13',
    'NORTH BERNAL HTS':'NEIG14',
    'NORTH EMBARCADERO':'NEIG15',
    'PORTOLA':'NEIG26',
    'POTRERO HILL':'NEIG16',
    'RICHMOND':'NEIG17',
    'SEA CLIFF/PRESIDIO HEIGHTS':'NEIG18',
    'SOUTH BERNAL HEIGHT':'NEIG19',
    'SOUTH OF MARKET':'NEIG20',
    'SUNSET':'NEIG21',
    'UPPER MARKET/EUREKA VALLEY':'NEIG22',
    'VISITATION VALLEY':'NEIG23',
    'WEST OF TWIN PEAKS':'NEIG24',
    'WESTERN ADDITION':'NEIG25',
    'BAYVW/HTRSPT':'NEIG1',
    'CHINA':'NEIG2',
    'CVC CTR/DWTN':'NEIG3',
    'DIAMD HTS':'NEIG4',
    'EXCELSIOR':'NEIG5',
    'HAIGHT ASH':'NEIG6',
    'LRL HTS/ANZA':'NEIG10',
    'MAR/PAC HTS':'NEIG11',
    'N BERNAL HTS':'NEIG14',
    'N EMBRCDRO':'NEIG15',
    'RICHMOND':'NEIG17',
    'SECLF/PREHTS':'NEIG18',
    'S BERNAL HTS':'NEIG19',
    'SOMA':'NEIG20',
    'UPRMKT/EURKA':'NEIG22',
    'VISITA VLY':'NEIG23',
    'W TWIN PKS':'NEIG24',
    'WST ADDITION':'NEIG25',
}

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
    parser.add_argument('-d', '--debug', action='store_true',
                        help='enable debug info printout')
    parser.add_argument('-p', dest='pipe', action='store_true',
                        help='use pipe separator else tab')
    parser.add_argument('-P', dest='pretty', action='store_true',
                        help='pretty-print json output')
    parser.add_argument('-Z', dest='zero', action='store_true',
                        help='make a zero report')
    parser.add_argument('-s', dest='dirsuffix',
                        help='set the output directory (../out-orr default)')
    parser.add_argument('-M', dest='nombpct', action='store_true',
                        help='skip ED report for MB precincts')
    parser.add_argument('-z', dest='withzero', action='store_true',
                        help='include precincts with zero voters')

    args = parser.parse_args()
    # Force withzero for now.
    # args.withzero = True

    return args

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

config = Config(CONFIG_FILE, valid_attrs=config_attrs)
CardContest = {}
CardTurnOut = []
if config.card_turnout_contests:
    for i,v in enumerate(config.card_turnout_contests):
        CardContest[v] = i+1

have_EDMV =  not config.all_mail_election
# Bug in SOV: Cumulative not computed
grand_totals_wrong = config.grand_totals_wrong


if args.verbose:
    print(f"all_mail_election={config.all_mail_election} have_EDMV={have_EDMV}")

# Load translations
translator = Translator(TRANSLATIONS_FILE)

#print(config.card_turnout_contests)

json_dump_args = PP_JSON_DUMP_ARGS if args.pretty else DEFAULT_JSON_DUMP_ARGS

separator = "|" if args.pipe else "\t"

file_sha = {}
infile_sha = {}
load_sha_filename(SHA_FILE_NAME, file_sha)
load_sha_filename("../../state/vr/"+SHA_FILE_NAME, file_sha, "vr/")
load_sha_filename("../vr/"+SHA_FILE_NAME, file_sha, "vr/")
load_sha_filename("../omniballot/"+SHA_FILE_NAME, file_sha, "omniballot/")

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

def append_sha_list(
    filename: str      # File name read
    ):
    """
    Adds the file_sha entry to infile_sha_list if it exists
    """
    global infile_sha, file_sha
    print(f"append_sha_list({filename})={file_sha.get(filename,'')}")
    if filename in file_sha:
        infile_sha[filename] = file_sha[filename]



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

def putfilea(
    filename: str,      # File name to be appended
    headerline: str,    # First line with field names (without \n)
    datalist: List[str] # List of formatted lines with \n included
    ):
    """
    Opens a file for writing, emits the header line and sorted data
    """
    with open(filename,'a') as outfile:
        outfile.writelines(datalist)

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
        print(f"{errorPrefix} {linenum}:\n {foundHash[args[0]]}  {line}" )

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
    if args.debug:
        print(f"flushcontest({filename}) l={len(contest_arealines)}")
    if not contest_arealines:
        return
    with open(filename,'a' if readDictrict else 'w') as outfile:
        if separator != "|":
            headerline = re.sub(r'\|', separator, headerline)
        if not readDictrict:
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
    'Write-in':4,
    'Exhausted by Over Votes':0,
    'Overvotes':0,
    'Under Votes':1,
    'Blanks':1,
    'Exhausted':2,
    'Exhausted Ballots':2,
    'Continuing Ballots Total':3,
    'Continuing Ballots':3,
    'TOTAL':-1,
    'Non Transferable Total':-1,
    'REMARKS':-1,
    '':-1
    }

def checkDuplicate(d:Dict[str,str],  # Dict to set/check
                   key:str,          # Key
                   val:str,          # Value
                   msg:str):         # Message on duplicate
    if not check_duplicate_turnout:
        return
    global linenum
    if key not in d or d[key]=="0":
        d[key] = val
    elif d[key] != val:
        print(f"Duplicate {msg} for {key}->{val}!={d[key]} at {linenum}")
        raise

def addGrandTotal(grand_total,      # computed total lines
                  cols):            # Next subtotal line
    subtotal_type = cols[1]
    if subtotal_type not in grand_total:
        grand_total[subtotal_type] = ['ALL']+cols[1:]
    else:
        t = grand_total[subtotal_type]
        if len(t)!=len(cols):
            print(f"grand_total mismatch {t} != {cols}")
        for i in range(2,len(cols)):
            t[i] = int(cols[i])+int(t[i])


def loadEligible()->str:
    """
    Reads the ../vr/county.tsv file to locate eligible voters by county.
    Returns the count as a string, a numbmer or null.
    """
    try:
        vrfile = "../vr/county.tsv" if os.path.isfile("../vr/county.tsv") else "../../state/vr/county.tsv"
        with TSVReader(vrfile) as reader:
            append_sha_list("vr/county.tsv")
            for l in reader.readlines():
                if l[0] == "San Francisco":
                    return int(float(l[1]))
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
    # The | is used for readability
    sep = "|"
    # Pattern match the file names
    if (re2c.sub2ft(filename, r'.*supervisor\D+(\d+)$', 'd{0}_short.psv') or
        re2c.sub2ft(filename, r'^.*(district attorney).*$', 'da_short.psv') or
        re2c.sub2ft(filename, r'^.*(mayor|assessor|defender|attorney|sheriff|treasurer).*$', '{0}_short.psv')):
        filename = re2c.string
    else:
        raise FormatError(f"Unmatched RCV contest name {filename}")


    rcvtable = [ [] for i in range(len(candnames) + 5) ]
    rcvlines = []
    if filename not in zipfilenames:
        print(f"RCV file {filename} not found")
        return rcvlines, []
    global linenum
    linenumsave = linenum
    linenum = 0
    inHead = 1
    with rzip.open(filename) as f:
        append_sha_list(filename)
        i = 0
        for line in f:
            line = decodeline(line, SF_SOV_ENCODING)
            cols = line.split(sep)
            ncols = len(cols)

            # Skip to Candidate heading
            if inHead:
                if ncols>4 and cols[2]=='Votes':
                    # Record Votes columns
                    votecols = [i for i in range(ncols) if cols[i]=='Votes']
                    #print(f"voltecols={votecols}")
                    inHead = False
                continue

            # Filter list to
            candname = cols[0]

            if candname=='REMARKS': break

            if candname in rcvLabelMap:
                j = rcvLabelMap[candname]
                if j < 0: continue
            elif i>=len(candnames):
                continue
            else:
                #(party_name, writein, candname) = candnametrim(cols[0])
                if candnames[i] != candname:
                    raise FormatError(
                     f"Unmatched RCV candidate {candname}!={candnames[i]} in {filename}")
                j = i + 5
                i+=1
            rcvtable[j] = [int(float(cols[i])) for i in votecols]
            #print(f"{candname}:{rcvtable[j]}")
        # End loop over xls table rows
        #print(f"rcvtable={rcvtable}")

        # Transpose the data to decreasing RCV rounds
        rcvrounds = len(rcvtable[3])

        if args.zero:
            rcvrounds = 1
            final_cols = cols = ['RCV1']+statprefix+[0]*len(rcvtable)
            newtsvline(rcvlines, *cols)
            return rcvlines, final_cols

        # Insert 0 data with no columns filled
        for j in range(len(rcvtable)):
            if not rcvtable[j]:
                rcvtable[j] = [0] * rcvrounds

        if rcvrounds>1:
        # Check duplicate
            dup = 1
            for j in range(len(rcvtable)):
                if rcvtable[j][0] != rcvtable[j][1]:
                    dup = 0
                    break
            if dup:
                print(f"Duplicated: {contest_name}\n")
        else:
            dup = 0

        #print(f"rcvtable={rcvtable}")

        # Build the columns last round to first non-duplicate
        # The index i is 1 up, so list index 0 up is i-1
        for i in range(rcvrounds,dup,-1):
            # Set the area ID to RCV#
            area_id = f'RCV{i-dup}'
            # Clear fields above a 0 vote
            cols = [area_id]+statprefix+[ (rcvtable[j][i-1]
                            # Keep 0 in first round, UV/OV/Exh, else clear
                            if i<=1+dup or j<4 or rcvtable[j][i-1] != 0
                                        else '')
                                         for j in range(len(rcvtable)) ]
            if i==rcvrounds:
                final_cols = cols
            newtsvline(rcvlines, *cols)
        # End loop over rcv rounds
    # End processing html file
    linenum = linenumsave
    return rcvlines, final_cols

total_registration = 0
total_precinct_ballots = 0
total_mail_ballots = 0
total_precincts = 0
candbyname = {}

isrcv = set() # Contest IDs with RCV

no_voter_precincts = set() # IDs for precincts with no registered voters

#Load the contest ID map
# TODO: Remap to original DFM IDs
# Temporary - Use Omniballot contest ID, EDS candidate ID
have_contmap = os.path.isfile("contmap.tsv")
if have_contmap:
    with TSVReader("contmap.tsv") as r:
        append_sha_list("contmap.tsv")
        contmap = r.load_simple_dict(0,1)
    with TSVReader("candmap.tsv") as r:
        append_sha_list("candmap.tsv")
        candmap = r.load_simple_dict(0,1)
have_contmap = os.path.isfile("../omniballot/contmap.tsv")
if have_contmap:
    with TSVReader("../omniballot/contmap.tsv") as r:
        append_sha_list("omniballot/contmap.tsv")
        # Map ID to omniballot_id
        contmap_omni = r.load_simple_dict(1,0)
else:
    contmap_omni = {}

have_runoff = os.path.isfile("../omniballot/runoff-omni.tsv")
if have_runoff:
    with TSVReader("../omniballot/runoff-omni.tsv") as r:
        append_sha_list("omniballot/runoff-omni.tsv")
        runoff_by_contid = r.loaddict(2)
else:
    runoff_by_contid = {}

#Load countywide eligible voters
eligible_voters = loadEligible()

#Load approval_required
with TSVReader("../omniballot/contlist-omni.tsv") as r:
    append_sha_list("omniballot/contlist-omni.tsv")
    approval_required_by_omni_id = r.load_simple_dict(3,7)

#print("approval_required=",approval_required_by_omni_id)

def load_json_table(
    rzip,           # Opened CVR_Export zip file
    filename:str,   # JSON file to load
    attrs:dict,     # Expected attributes
    keyattr:str,    # Attribute for data key
    altdict:dict=None,   # Secondary dict or none
    altkeyattr:str=None, # Secondary dict index attriute
    )->Dict[str,object]: # Returns indexed object
    """
    Extract and load the json data to a parsed object. Create a tsv
    """
    with rzip.open(filename) as f:
        append_sha_list(filename)
        tsvlines = []
        tsvfilename = filename[:-5]+'.tsv'
        objtype = namedtuple(filename[:-5],attrs.keys())
        j = json.load(f)
        js_version = j['Version'];
        if js_version not in known_manifest_versions:
            print(f"Warning: {filename} version is {js_version} not known {known_manifest_versions}")
        d = {}
        for i in j['List']:
            for a,t in attrs.items():
                v = i.get(a)
                if v==None:
                    v = 0 if t==int else ""
                i[a] = t(v)
            r = objtype(**i)
            newtsvline(tsvlines,*r)
            d[i[keyattr]]=r
            if altdict!=None:
                altdict[i[altkeyattr]]=r
        putfile(filename[:-5]+'.tsv','|'.join(attrs.keys()),tsvlines)
    return d




# Extract Manifest data from the CVR json
ContestManifest_attrs= {
    "Description":str,
    "DistrictId":str, # New in 5.10
    "Id":str,
    "ExternalId":str,
    "VoteFor":int,
    "NumOfRanks":int,
    "Disabled":int,     # New in 5.10.50.85
   }

CandidateManifest_attrs= {
    "Description":str,
    "Id":str,
    "ExternalId":str,
    "ContestId":str,
    "Type":str,
    "Disabled":int,     # New in 5.10.50.85
    }

CandidateManifest = namedtuple("CandidateManifest",CandidateManifest_attrs.keys())

ContestManifest_by_Id = {}
ContestManifest = {}
CandidateManifest_by_ContestId = {}
CandidateManifest_by_Id = {}

known_manifest_versions = {"5.2.18.2", "5.10.11.24", "5.10.50.85"}

haveCVRExport = os.path.isfile("CVR_Export.zip")
if haveCVRExport:
    with ZipFile("CVR_Export.zip") as rzip:
        try:
            with rzip.open(SHA_FILE_NAME) as f:
                load_sha_file(f, file_sha)
        except:
            pass

        ContestManifest = load_json_table(rzip,"ContestManifest.json",
                            ContestManifest_attrs, "Description",
                            ContestManifest_by_Id, "Id")
        CandidateManifest_by_Id = load_json_table(rzip,"CandidateManifest.json",
                            CandidateManifest_attrs, "Id");

        for r in CandidateManifest_by_Id.values():
            if not r.ContestId in CandidateManifest_by_ContestId:
                CandidateManifest_by_ContestId[r.ContestId] = {}
            CandidateManifest_by_ContestId[r.ContestId][r.Description.upper()] = r


# Process the registration and turnout stored in turnoutdata-raw.zip

Party_IDs = ['AI','DEM','GRN','LIB','PF','REP','NPP']

EWM_header = "Voting Precinct|Polls|VBM|Total"
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


VBM_turnout = {}
EWM_turnout = {}

with ZipFile("turnoutdata-raw.zip") as rzip:
    zipfilenames = get_zip_filenames(rzip)


    if "ewmr057.psv" in zipfilenames:
        with TSVReader("ewmr057.psv",opener=rzip,binary_decode=True,
                       validate_header=EWM_header) as f:
            EWM_turnout = f.loaddict()

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

# Load turnout by party
turnoutfile = f'{OUT_DIR}/turnout.tsv'
partyTurnout = []
have_turnout = os.path.isfile(turnoutfile)
if have_turnout:
    with TSVReader(turnoutfile) as f:
        # cols area_id|subtotal_type|party|RSReg|RSCst|...
        partyHeaders = f.header[3:]
        partyTurnout.append('\t'.join(f.header))
        for cols in f.readlines():
            area_id, subtotal_type, party, RSReg, RSCst = cols[:5]
            if area_id=='ALL':
                partyTurnout.append('\t'.join(cols))
            if party=='ALL':
                party=''

            if RSReg=='':
                continue

            if subtotal_type=='TO':
                RSRegSave_TO[area_id+party] = int(RSReg)
            elif subtotal_type=='MV':
                RSRegSave_MV[area_id+party] = int(RSReg)
                RSCstSave_MV[area_id+party] = int(RSCst)
            else:
                # Skip ED
                continue


# Now RSRegSave_MV and RSRegSave_TO have VBM and total registration
# by area_id or area_id+party suffix. The party+NPP suffix has VBM issued
# area_id+NPPALL has NPP combined ballots issued and returned. Total
# issued and returned for crossover parties has a DEMALL etc.

# Vote by mail registration is found in the vbmprecinct.csv along with
# a breakdown by party and precinct. We save the MV "registration" for
# all voters in each precinct. We define MV registration as mail ballots
# issued, so is permanent vote by mail plus requested mail ballots. These
# are not eligible for ED voting (unless the mail ballot is surrendered),
# so we define ED registration as the difference between mail ballots issued
# and total registration

# Compute countywide MV registration by summary district
# Precincts for each summary district are stored sdistpct[SUPV4]=[PCT9401...]
# Summary district for each precinct sdistpct[PCT9401]=[CONG12...]
sdistpct:Dict[Area_Id,List[Area_Id]] = {}

# Contests may be a subset of the whole county, e.g. supervisorial district,
# so have a subset of voters when the contest district crosses the summary district
# The partial
# RSRegSummary['ASSM17']['NEIG12']['MV'] = 12,345
# So RSRegSummary['ASSM17'] has a list of summary districts for contests
# that are in ASSM17, eg. NEIG12 is one
RSRegSummary:Dict[str,Dict[str,Dict[str,int]]] = {}


if os.path.isfile("sdistpct.tsv"):
    with TSVReader("sdistpct.tsv",validate_header="area_id|precinct_ids") as f:
        for (area_id, precinct_ids) in f.readlines():
            # Convert precinct number to area ID
            precincts = ['PCT'+s for s in precinct_ids.split()]
            # Save the precinct list for contest-district tabulation
            sdistpct[area_id] = precincts

if  os.path.isfile("pctsdist.tsv"):
    with TSVReader("pctsdist.tsv",validate_header="precinct_ids|area_ids") as f:
        for (precinct_ids, area_ids) in f.readlines():
            # Save list of summary districts per precinct
            sdists = area_ids.split()
            for pct in precinct_ids.split():
                sdistpct['PCT'+pct] = sdists


# Process the downloaded SF results stored in resultdata-raw.zip
with ZipFile("resultdata-raw.zip") as rzip:
    # Create a set with the zip file names
    zipfilenames = get_zip_filenames(rzip)

    # Load sha256 hashes
    if SHA_FILE_NAME in zipfilenames:
        with rzip.open(SHA_FILE_NAME) as f:
            load_sha_file(f, file_sha)

    #print(f'file_sha={file_sha}\n')

    # Read the release ID and title
    results_id = ''
    results_title = ''
    if "lastrelease.txt" in zipfilenames:
        with rzip.open("lastrelease.txt") as f:
            append_sha_list("lastrelease.txt")
            last_release_line = f.read().decode('utf-8').strip()
            m = re.match(r'(.*):(.*)', last_release_line)
            if m:
                results_id, results_title = m.groups()
            else:
                print(f'Unmatched lastrelease.txt:{last_release_line}')

    # Read turnout details

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

    # Read the summary psv file
    # Extract summary_reporting[contest_id] and summary_precincts[contest_id]
    sep = '|'
    sovfile = "summary.psv"
    if (sovfile not in zipfilenames and
        "summary0.psv" in zipfilenames):
        sovfile = "summary0.psv"
    summary_reporting = {}
    summary_precincts = {}
    party_suffix_pat = re2(r' *␤(\w+)')
    if sovfile in zipfilenames:
        with rzip.open(sovfile) as f:
            append_sha_list(sovfile)
            contest_id = 'TURNOUT'
            precincts_reported_pat = re2(r'Precincts Reported: (\d+) of (\d+)')
            linenum = 0
            for line in f:
                line = decodeline(line, SF_SOV_ENCODING)
                # contest_party is appended to the contest name, so trim it!
                line =  party_suffix_pat.sub('',line)
                contest_party = party_suffix_pat[1]
                if precincts_reported_pat.match(line):
                    if not contest_id:
                        print(f"summary contest name mismatch {linenum}:{line}")
                        continue
                    (summary_reporting[contest_id],
                     summary_precincts[contest_id])=precincts_reported_pat.groups()
                    contest_id = ''
                    continue
                if line in ContestManifest:
                    if contest_id != '':
                        print(f"summary precincts reported mismatch {linenum}:{line}")
                    contest_id = ContestManifest[line].Id

    # Read the SOV results
    have_dsov = "dpsov.psv" in zipfilenames or "dsov.psv" in zipfilenames
    print(f"have_dsov={have_dsov}")
    for readDictrict in [False, True]:
      for sovfile in ["sov.psv","sov.tsv","psov.psv","psov.tsv",'']:
        if readDictrict:
            sovfile = 'd'+sovfile

        if sovfile in zipfilenames:
            sep = '|' if sovfile.endswith("psv") else '\t'
            break
      if sovfile=='':
            raise FormatError(f"Unmatched SOV XLSX file")
      if sovfile=='d':
          print("No dsov file found.")
          break
      if args.verbose:
        print(f"Reading {sovfile}")
      with rzip.open(sovfile) as f:
        append_sha_list(sovfile)
        linenum = 0
        candlist = []
        contlist = []
        pctlist = []
        tallylist = []
        contest_name = ''
        in_turnout = False
        subtotal_col = -1
        pctname2areaid = {} # Map original precinct ID to area ID

        have_EDMV =  not config.all_mail_election

        if readDictrict:
            col0_header = 'District'
        else:
            col0_header = 'Precinct'


        # Patterns for line type
        page_header_pat = re2(r'\f?Page: (\d+) of \d+[\|\t]+(20\d\d-\d\d-\d\d[ :\d]+)$')
        contest_header_pat=re2(r'^(.*)(?: \(Vote for +(\d+)\))?$')
        precinct_name_pat = re2(r'^(?:Pct|PCT) (\d+)(?:/(\d+))?( MB)?$')
        subtotal_name_pat = re2(r'^(Election Day|Vote by Mail|Total)$')
        # Senate omitted to skip
        district_category_pat=re2(r'^(United States Representative|Member of the State Assembly|County Supervisor|Neighborhood|CONGRESSIONAL|ASSEMBLY|SUPERVISORIAL|NEIGHBORHOOD)(?:$|[\|\t])')
        # If grand_totals_wrong compute Cumulative
        skip_area_pat_str = (r'^(Cumulative|Cumulative - Total|Countywide|Countywide - Total|City and County - Total)$'
                         if (not have_EDMV or grand_totals_wrong) and not readDictrict else
                         r'^(Electionwide|San Francisco|Cumulative|Countywide|City and County) - Total$')
        skip_area_pat = re2(skip_area_pat_str)
        heading_pat = re2(r'Statement of the Vote(?: -)?( \d+)?(.Districts and Neighborhoods)?$')

        writeincand_suffix = "␤Qualified Write In"
        TURNOUT_LINE_SUFFIX = '% Turnout'

        # Collected precinct consolidation
        pctcons = []        # precinct consolidation tsv
        foundpctcons = {}   # pctcons lines by ID
        contstats = []      # Table of vote stats by contest ID
        pctcontest = {}     # Collected precincts by contest
        cont_id_eds2sov = {}# Map an eds ID to sov ID
        pctturnout = []
        precinct_count = {}
        pctturnout_reg = {} # Map original precinct ID to registration
        pctturnout_ed = {}  # Map original precinct ID to ED ballots
        pctturnout_mv = {}  # Map original precinct ID to MV ballots

        # Note:Not in current files
        district_id_pat = re2(r'^(\w+):(.+)')

        # Used to make ID/sequence
        contest_order = 0
        skip_lines = 0
        skip_to_category = next_is_district = False
        contest_name=''
        withzero = args.withzero
        zero_voter_contest = False

        if args.debug:
            print(f"skip_area_pat={skip_area_pat_str}")

        for line in f:
            line = decodeline(line, SF_SOV_ENCODING)
            if line == "": continue
            if heading_pat.search(line):
                zero_report = heading_pat.group(1)==' 0'
                #print(f"zero_report={zero_report}")
                # Ignore heading lines
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
                if page == '1':
                    results_json["_reporting_time"]=report_time_str
                if (readDictrict or not have_dsov) and contest_name:
                    flushcontest(contest_order, contest_id, contest_name,
                                headerline, contest_rcvlines,
                                contest_totallines, contest_arealines)

                contest_name=''
                skip_to_category = next_is_district = False
                continue

            if skip_to_category:
                if district_category_pat.match(line):
                    district_category = district_category_pat[1]
                    skip_to_category = False
                    next_is_district = True
                continue

            line = re.sub(r'Registered ?␤Voters','Registered Voters',line)

            if contest_name=='':
                if args.debug: print(f"Heading line {linenum}:'{line}'")
                if line.endswith(TURNOUT_LINE_SUFFIX):
                    in_turnout = True
                    contest_id = contest_name = 'TURNOUT'
                    contest_party = ''
                elif contest_header_pat.match(line):
                    contest_name, vote_for = contest_header_pat.groups()
                    # Trim duplicate party suffix
                    contest_name =  party_suffix_pat.sub('',contest_name)
                    contest_party = party_suffix_pat[1]
                    contest_order += 1
                    contest_order_str = str(contest_order).zfill(3)
                    if not vote_for:
                        vote_for = 1
                    vote_for = int(vote_for)
                    if contest_name not in ContestManifest:
                        if haveCVRExport:
                            print(f"Unmatched contest {contest_name}")
                        contest_id = 'X'+contest_order_str
                        candbyname = {}
                        max_ranked = 0
                        contest_id_ext = 0
                    else:
                        contest = ContestManifest[contest_name]
                        contest_id = contest.Id
                        contest_id_ext = contest.ExternalId
                        vote_for = contest.VoteFor
                        candbyname = CandidateManifest_by_ContestId.get(contest_id,{})
                        max_ranked = contest.NumOfRanks
                        if contest.VoteFor != vote_for:
                            print(f"Mismatched VoteFor in {contest_name}: {contest.VoteFor} != {vote_for}")
                    if max_ranked > 0:
                        isrcv.add(contest_id)
                    omni_id = contmap_omni.get(contest_id,contest_id)
                    contline = jointsvline(contest_order_str, contest_id,
                                           contest_id_ext,
                                contest_name, vote_for, max_ranked)
                    contlist.append(contline)
                    if re.match(r'.*House of Rep District 13',contest_name):
                        #print(f"**zero_voter_contest=True {contest_name}")
                        zero_voter_contest = True
                    else:
                        zero_voter_contest = False
                else:
                    raise FormatError(f"sov contest name mismatch {linenum}:{line}")


                card = CardContest.get(contest_id,0)
                if card:
                    #Create an empty list to save turnout by subtotal and precinct
                    CardTurnOut.append(dict(TO={},ED={},MV={}))
                grand_total = {} # Computed totals
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
                total_precinct_ballots = total_mail_ballots = 0
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
                last_cand_col = 0
                expected_cols = ncols
                skip_to_category = readDictrict
                if cols[0] != col0_header:
                    raise FormatError(f"sov column 0 header mismatch {linenum}:{line}")

                if in_turnout:
                    if (cols[1] != 'Registered Voters' or
                        cols[2] or cols[3] != 'Cards Cast' or cols[4] or
                        cols[5] != 'Voters Cast' or
                        cols[6] != '% Turnout'):
                        raise FormatError(f"sov turnout column header mismatch {linenum}:{line}")
                    rs_group = 'EMT'
                    hasrcv = haswritein = 0
                else:
                    total_col = -1
                    writein_col = 0
                    if (cols[1] == 'Times Cast' and
                        cols[2] == 'Registered Voters' and
                        cols[3] == '' and
                        cols[4] == 'Undervotes' and
                        cols[5] == 'Overvotes' and
                        cols[6] == col0_header):
                        have_EDMV = not config.all_mail_election
                        have_RSCst = True
                        have_RSReg = True
                        cand_start_col = 7
                        subtotal_col = 0

                    elif (cols[1] == 'Registered Voters' and
                          cols[2] == 'Undervotes' and
                          cols[4] == 'Overvotes' and
                          cols[5] == col0_header):
                        #print("WARNING:have_EDMV = False")
                        # Maybe only in the zero report
                        have_EDMV = not config.all_mail_election

                        have_RSReg = True
                        have_RSCst = False
                        cand_start_col = 6

                    elif (cols[1] == 'Undervotes' and
                          cols[2] == 'Overvotes' and
                          cols[4] == col0_header):
                        #print("WARNING:have_EDMV = False")
                        # Maybe only in the zero report
                        have_EDMV = not config.all_mail_election
 # False in prior zero reports
                        have_RSCst = have_RSReg = False
                        cand_start_col = 5
                    else:
                        raise FormatError(f"sov column header mismatch {linenum}:{cols}")

                    for i in range(cand_start_col, ncols):
                        name = cols[i]
                        if name == '':
                            continue
                        if name == 'Write-in':
                            heading = 'RSWri'
                            writein_col = i
                        elif name == 'Total Votes':
                            total_col = i
                            continue
                        else:
                            is_writein_candidate = False
                            last_cand_col = i
                            if name.endswith(writeincand_suffix):
                                is_writein_candidate = True
                                name = name[:-len(writeincand_suffix)]
                                #print(f"Writein {name} {i}/{ncols}")
                                if i+1 == ncols:
                                    # Number of columns will be ncols+1 for percent
                                    # Bogus columns get inserted into the XLS
                                    # So sometimes it's ncols+2
                                    expected_cols = i+2
                                #continue
                            name = re.sub(r'␤\(\w+\)$','',name)
                            cand_order += 1
                            candidx.append(i)
                            candidate_order = len(candidx)
                            candidate_order_str = str(candidate_order).zfill(2)
                            candidate_full_name = name
                            candnames.append(name)
                            candidate_party_id = ""
                            NAME = name.upper()
                            if NAME not in candbyname:
                                if haveCVRExport:
                                    print(f"Can't match {name} in {contest_id}:{contest_name}");
                                cand_id = f'{contest_id}{candidate_order:02}'
                                candidate_type = ""
                                if is_writein_candidate:
                                    candbyname[NAME] = CandidateManifest(
                                        name, cand_id, cand_id,
                                        contest_id, "WriteIn")
                            else:
                                candidate = candbyname[NAME]
                                cand_id = candidate.Id
                                candidate_type = candidate.Type
                                is_writein_candidate = re.search(
                                    r'Write ?in', candidate_type, flags=re.I) != None

                            candheadings.append(f'{cand_id}:{name}')
                            candids.append(cand_id)

                            candline = jointsvline(contest_id,
                                            candidate_order_str, cand_id,
                                            candidate_type,
                                            candidate_full_name,
                                            candidate_party_id,
                                            boolstr(is_writein_candidate))
                            candlist.append(candline)
                        # End loop over candidate names
                    if total_col < 0:
                        raise FormatError(f"sov column header mismatch (no total) {linenum}:{cols}")

                    haswritein = writein_col > 0
                    # Check for an RCV contest
                    hasrcv = contest_id in isrcv
                    if hasrcv:
                        rs_group = "EMR"
                    else:
                        rs_group = "EMC"

                    #Compute result stat group
                    if haswritein:
                        rs_group += "W"

                    if args.verbose:
                        print(f"Contest({contest_id}:{rs_group} v={vote_for} {contest_name})")

                resultlist = resultlistbytype[rs_group]
                headers = ['area_id','subtotal_type'
                           ] + resultlist + candheadings;
                headerline = jointsvline(*headers)
                if args.debug:
                    print(f"subtotal_col={subtotal_col} headerline={headerline}")


            else:
                # Normal data line
                # Column of data
                if skip_area_pat.match(cols[0]) and not readDictrict:
                    # Superflous totals
                    if args.debug: print(f"Skip {linenum}:{line}")
                    skip_area = True
                    continue

                pct_col_0 = True
                if cols[0] == "Cumulative":
                    if readDictrict:
                        skip_to_category = True
                        continue
                    area_id = "ALL"
                    isvbm_precinct = False
                    skip_area = False
                    #if args.debug: print(f"ALL at {linenum}")
                    continue
                elif re.match(r'^(San Francisco|Electionwide) - Total',cols[0]):
                    area_id = "ALL"
                    isvbm_precinct = False
                    subtotal_col = -1
                    skip_area = True
                    #if args.debug: print(f"ALL at {linenum}:{line}")

                elif next_is_district:
                    # This should be a district heading
                    if (ncols!=1 and
                        cols[0]!=cols[-1]):
                        raise FormatError(f"sov district heading mismatch={ncols} {cols} {linenum}:{line}")
                    name = cols[0]
                    #Reform name
                    name = re.sub(r'^(\d+)(ST|ND|RD|TH) (.+)',
                                  r'\3 \1',name,flags=re.I)
                    #if in_turnout:
                        #print(name,file=df)
                    area_id = distcodemap.get(name,'???')
                    if area_id=='???':
                        print(f"Can't map district code {name}")
                    next_is_district = False
                    skip_area = area_id == 'CONG13'
                    have_EDMV = not config.all_mail_election
                    subtotal_col = 0
                    continue

                elif precinct_name_pat.match(cols[0]):
                    # Area is a precinct
                    skip_area = False
                    # re.match(r'PCT (\d+)(?:/(\d+))?( MB)?$', cols[0])
                    (precinct_id, precinct_id2, vbmsuff
                     ) = precinct_name_pat.groups()
                    area_id = "PCT"+precinct_id
                    isvbm_precinct = vbmsuff != ""
                    precinct_name = precinct_name_orig = cols[0]
                    pctname2areaid[precinct_name_orig] = area_id
                    # Clean the name
                    precinct_name = re.sub(r'^PCT','Precinct',precinct_name,flags=re.I)
                    pctlist.append(precinct_id)
                    if precinct_id2:
                        pctlist.append(precinct_id2)

                    # Check precinct consolidation
                    if precinct_id2:
                        cons_precincts = precinct_id+" "+precinct_id2
                        cons_pct_count = 2
                    else:
                        cons_precincts = precinct_id
                        cons_pct_count = 1

                    if ncols==1:
                        have_EDMV = True
                        subtotal_col = 0
                        if config.all_mail_election:
                            print(f"have_EDMV cols={cols}")
                    if have_EDMV:
                        if ncols == expected_cols:
                            raise FormatError(
            f"Mismatched Precinct Column count {ncols}!=1 {linenum}:{line}")
                        subtotal_col = 0
                        continue

                else:
                    pct_col_0 = False

                #if args.debug:
                    #print(f"cols={cols}")

                if readDictrict and cols[0].endswith(" - Total"):
                    cols[0] = "Total"
                    next_is_district = True

                if (expected_cols != ncols and not
                    # column length can vary with writein at the end to
                    # add 0, 1 or 2 extra columns for percentage. The
                    # percentage can be omitted with no votes, or if nonzero
                    # is added, but sometimes there are extra blank columns
                    # mysteriously inserted into the xls. :-(P
                    (is_writein_candidate and
                      last_cand_col < ncols and last_cand_col+3 >= ncols)):
                    print(f"pct_col_0={pct_col_0} have_EDMV={have_EDMV} area_id={area_id} precinct_name={precinct_name} readDictrict={readDictrict}")
                    raise FormatError(f"Mismatched Column count {ncols}!={expected_cols}:{last_cand_col} {linenum}:{line}")

                if args.zero:
                    for i in range(3,len(cols)):
                        cols[i] = "0"
                    if in_turnout or not have_EDMV:
                        cols[2] = "0"
                    else:
                        cols[1] = "0"

                def convRSOvr(RSOvr):
                    return int(int(RSOvr)/vote_for)

                subtotal_type = 'TO'
                if subtotal_col >= 0:
                    subtotal_type = vgnamemap.get(cols[subtotal_col],'')

                if in_turnout:
                    (RSReg, RSCards, RSCst) = [int(float(cols[i])) for i in [1,3,5]]
                    RSTrn = cols[6]
                    RSRej = 0 # Not available
                    if RSTrn == "N/A":
                        RSTrn = "0.0";
                elif not have_RSReg:
                    (RSUnd, RSOvr) = [int(float(cols[i])) for i in [1,2]]
                    RSOvr = convRSOvr(RSOvr)
                    total_votes = RSTot = int(float(cols[total_col]))
                    RSCst = total_ballots = int(RSOvr)+int((int(RSTot)+int(RSUnd))/vote_for)
                    if area_id == 'ALL':
                        # Use computed totals
                        RSReg = grand_total[subtotal_type][2] if grand_total else 0
                    else:
                        area_party = area_id
                        if contest_party:
                            area_party+=contest_party
                        RSReg = RSRegSave[subtotal_type].get(area_party,None)
                        if RSReg==None:
                            #print(f"RSRegSave[{subtotal_type}][{area_party}]={RSRegSave[subtotal_type].get(area_party,None)}")
                            RSReg = RSRegSave_TO.get(area_party,RSRegSave_TO[area_id])
                    # RSRej not available
                    #print(f"{area_id}:{subtotal_type} {RSCst}/{RSReg}")
                    RSRej = RSExh = 0
                elif not have_RSCst:
                    (RSReg, RSUnd, RSOvr) = [int(float(cols[i])) for i in [1,2,4]]
                    RSOvr = convRSOvr(RSOvr)
                    total_votes = RSTot = int(float(cols[total_col]))
                    RSCst = total_ballots = int(RSOvr)+int((int(RSTot)+int(RSUnd))/vote_for)
                    # RSRej not available
                    RSRej = RSExh = 0
                else:
                    (RSCst, RSReg, RSUnd, RSOvr) = [int(float(cols[i])) for i in [1,2,4,5]]
                    RSOvr = convRSOvr(RSOvr)
                    total_votes = RSTot = int(float(cols[total_col]))
                    total_ballots = int(RSOvr)+int((int(RSTot)+int(RSUnd))/vote_for)
                    RSRej = int(int(RSCst)-total_ballots)
                    # RSRej not available
                    RSExh = 0

                no_voter_precinct = (area_id in RSRegSave_TO and
                                     RSRegSave_TO[area_id]==0 and
                                     not (zero_voter_contest or
                                          isvbm_precinct or withzero or zero_report))

                #if RSReg==0:
                    #print(f"no_voter_precinct {no_voter_precinct} {area_id} {contest_party} {subtotal_type}")

                if pct_col_0:
                    pass
                elif not subtotal_name_pat.match(cols[0]):
                    # Unmatched Area
                    print(f"skip_area={skip_area} area_id={area_id} readDictrict={readDictrict}")
                    raise FormatError(f"sov area mismatch {linenum}: {line}")
                elif skip_area:
                    continue
                elif zero_voter_contest and readDictrict:
                    continue

                if cons_precincts and not contest_party:
                    newtsvlineu(foundpctcons, pctcons,
                                "Mismatched Precinct Consolidation",
                                precinct_id, precinct_name,
                                boolstr(isvbm_precinct),
                                boolstr(RSReg==0),
                                cons_precincts)
                    cons_precincts = ''
                # Map the subtotal_type
                if subtotal_col >= 0:
                    if subtotal_type == 'ED':
                        if area_id != "ALL":
                            checkDuplicate(pctturnout_reg, precinct_name_orig, RSReg,
                                        "Registration")
                            checkDuplicate(pctturnout_ed, precinct_name_orig, RSCst,
                                        "Election Day Turnout")
                            if args.nombpct and isvbm_precinct and not RSCst:
                                continue
                            if no_voter_precinct:
                                if RSCst:
                                    print(f"Skipped ED NV precinct {linenum}: {line}")
                                continue    # Skip ED reporting for VBM-only precincts
                            ed_precincts += 1
                    elif subtotal_type == 'MV':
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
                                no_voter_precincts.add(area_id)
                                continue
                        ## End not all precincts
                    elif subtotal_type != 'TO':
                        raise FormatError(f"subtotal name mismatch {linenum}: {line}")
                if no_voter_precinct and subtotal_type == 'TO':
                    no_voter_precincts.add(area_id)
                    RSRegSave[subtotal_type][area_id] = 0
                    continue
                if in_turnout:
                    if have_turnout and area_id in RSRegSave_MV:
                        #Convert total registration to MV & ED, MV is first
                        if isvbm_precinct:
                            if subtotal_type == 'ED':
                                RSReg = RSRegSave_ED[area_id] = 0
                                for p in Party_IDs:
                                    RSRegSave_ED[area_id+p] = 0
                            else:
                                RSRegSave_MV[area_id] = RSRegSave_TO[area_id] = RSReg
                        elif subtotal_type == 'MV':
                            RSRegSave_ED[area_id] = RSReg - RSRegSave_MV[area_id]
                            RSReg = RSRegSave_MV[area_id]
                        elif subtotal_type == 'ED':
                            RSReg = RSRegSave_ED[area_id] = RSReg - RSRegSave_MV[area_id]
                        else:
                            RSRegSave_TO[area_id] = RSReg
                            for p in Party_IDs:
                                area_party = area_id+p
                                if area_party not in RSRegSave_TO:
                                    continue
                                RSRegSave_ED[area_party] = subQ(
                                    RSRegSave_TO[area_party],
                                    RSRegSave_MV[area_party])

                        if subtotal_type == 'MV' or subtotal_type == 'ED':
                             for p in Party_IDs:
                                area_party = area_id+p
                                if area_party not in RSRegSave_TO:
                                    continue
                                RSRegSave_ED[area_party] = subQ(
                                    RSRegSave_TO[area_party],
                                    RSRegSave_MV[area_party])

                        #print(f"{subtotal_type}:RSRegSave_MV[{area_id}]={RSRegSave_MV[area_id]}/{RSReg}")
                    else:
                        RSRegSave[subtotal_type][area_id] = RSReg

                    stats = [area_id, subtotal_type, RSReg, RSCst]
                    outline = jointsvline(*stats)
                    if area_id.startswith("PCT") and (
                        subtotal_col <0 or
                        subtotal_type == ('MV' if isvbm_precinct else 'ED')):
                        total_precincts += 1
                        if RSCst:
                            processed_done += 1

                    if area_id != "ALL":
                        addGrandTotal(grand_total,stats)
                    elif readDictrict:
                        continue
                    else:
                        if grand_totals_wrong:
                            total_precinct_ballots = grand_total['ED'][3]
                            total_mail_ballots = grand_total['MV'][3]
                            RSRegSave_ED['ALL'] = total_precinct_registration = grand_total['ED'][2]
                            RSRegSave_ED['MV'] = total_mail_registration = grand_total['MV'][2]

                            #print(f'Turnout grand totals {total_precinct_ballots}/{total_mail_ballots}\n')
                        else:
                            if subtotal_type == 'TO':
                                total_precinct_ballots = RSCst
                            elif subtotal_type == 'MV':
                                total_mail_ballots = RSCst
                            elif RSCst != total_precinct_ballots+total_mail_ballots:
                                print(f"Turnout discrepancy {RSCst} != {total_precinct_ballots}+{total_mail_ballots}")
                            # Validate subtotal
                            if RSCst != grand_total[subtotal_type][3]:
                                print(f"Turnout discrepancy {subtotal_type} {RSCst}!={grand_total[subtotal_type][3]}")
                    if subtotal_type == 'ED':
                        total_precinct_ballots = RSCst
                    elif subtotal_type == 'MV':
                        total_mail_ballots = RSCst
                    else:
                        if have_EDMV:
                            if area_id not in RSRegSave_ED:
                                RSRegSave_ED[area_id] = RSReg
                            if area_id not in RSRegSave_MV:
                                RSRegSave_MV[area_id] = RSReg
                            newtsvline(pctturnout, area_id, RSReg,
                                RSRegSave_ED[area_id], RSRegSave_MV[area_id],
                                RSCst, total_precinct_ballots, total_mail_ballots)
                        else:
                            newtsvline(pctturnout, area_id, RSReg,
                                RSCst)


                    if area_id == "ALL" and subtotal_type == 'TO':
                        # Enter turnout
                        total_registration = RSReg
                        processed_done = summary_reporting.get("TURNOUT",processed_done)
                        total_precincts = summary_precincts.get("TURNOUT",total_precincts)
                        if RSCst != total_precinct_ballots+total_mail_ballots:
                            print(f"Turnout discrepancy {RSCst} != {total_precinct_ballots+total_mail_ballots} ({total_precinct_ballots}+{total_mail_ballots})")
                        if have_EDMV:
                            js_turnout = results_json['turnout'] = {
                            "_id": "TURNOUT",
#                            "no_voter_precincts": nv_pctlist,
                            "precincts_reporting": int(processed_done),
                            "total_precincts": int(total_precincts),
                            "eligible_voters": int(eligible_voters),
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
                                    "results": [total_registration, total_precinct_registration,
                                                total_mail_registration]
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
                        else:
                            js_turnout = results_json['turnout'] = {
                            "_id": "TURNOUT",
#                            "no_voter_precincts": nv_pctlist,
                            "precincts_reporting": int(processed_done),
                            "total_precincts": int(total_precincts),
                            }
                            if ADD_RESULTS_VECTOR:
                                js_turnout["result_stats"]= [
                                {
                                    "_id": "RSEli",
                                    "heading": "Eligible Voters",
                                    "results": [ eligible_voters]
                                },
                                {
                                    "_id": "RSReg",
                                    "heading": "Registered Voters",
                                    "results": [int(RSRej)]
                                },
                                {
                                    "_id": "RSCst",
                                    "heading": "Ballots Cast",
                                    "results": [int(RSCst)]
                                },
                                {
                                    "_id": "RSRej",
                                    "heading": "Ballots Challenged",
                                    "results": [int(RSRej)]
                                }
                            ]
                        # Unused: "reporting_time": report_time_str,

                        if partyTurnout:
                            js_turnout['results_summary'] = partyTurnout
                    continue
                else:
                    if hasrcv:
                        stats = [area_id, subtotal_type, RSReg, RSCst,
                                RSRej, RSOvr, RSUnd, RSExh, RSTot]
                    else:
                        stats = [area_id, subtotal_type, RSReg, RSCst,
                                RSRej, RSOvr, RSUnd, RSTot]
                    if haswritein:
                        stats.append(int(float(cols[writein_col]))) # First write-in
                    cand_start_col = len(stats)
                    stats.extend([int(float(cols[i])) for i in candidx])

                    if card:
                        CardTurnOut[card-1][subtotal_type][area_id] = RSCst

                    if area_id.startswith("PCT") and (
                        subtotal_col <0 or
                        subtotal_type == ('MV' if isvbm_precinct else 'ED')):
                        total_precincts += 1
                        if RSTot:
                            processed_done += 1

                outline = jointsvline(*stats)
                if area_id == "ALL":
                    if args.debug:
                        print(f"ALL:{subtotal_type} at {linenum}")

                    if subtotal_type == 'TO':
                        # Put grand totals first
                        if grand_totals_wrong :
                            # Some MB precincts have missing ED registration
                            contest_totallines.append(jointsvline(*grand_total['ED']))
                            contest_totallines.append(jointsvline(*grand_total['MV']))
                            outline2 = jointsvline(*grand_total['TO'])
                            if outline != outline2:
                                print(f"grand_total discrepancy for {contest_id} \n   {outline}\n   {outline2}")

                        contest_totallines.insert(0, outline)

                        # Save/Check total [s for results summary
                        candvotes = stats[cand_start_col:]
                        if hasrcv:
                            # stats:subtotal_type, RSReg, RSCst, RSRej,
                            contest_rcvlines, final_cols = loadRCVData(
                                rzip, contest_name, candnames, stats[1:5])
                            if final_cols:
                                candvotes = final_cols[cand_start_col:]
                            #else:
                               #hasrcv = False
                        else:
                            contest_rcvlines = []
                        rcv_rounds = len(contest_rcvlines)
                        winning_status = {}
                        cand_success = {}   # True/False for winning_status

                        processed_done = summary_reporting.get(contest_id,processed_done)
                        total_precincts = summary_precincts.get(contest_id,total_precincts)

                        # Create json file output
                        conteststat = {
                            '_id': contest_id,
                            'heading':contest_name,
                            'precincts_reporting': int(processed_done),
                            'total_precincts': int(total_precincts),
                            }

                        #Unused: conteststat['reporting_time'] = report_time_str
#                        conteststat['no_voter_precincts'] = nv_pctlist
                        if hasrcv:
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
                                         int(approval_percent_pat.group(1)))//100)
                                else:
                                    votes_required = 0
                                if votes_required:
                                    if int(candvotes[0]) >= votes_required:
                                        conteststat['approval_met'] = True
                                        win_id = candids[0]
                                        lose_id = candids[1]
                                    else:
                                        conteststat['approval_met'] = False
                                        win_id = candids[1]
                                        lose_id = candids[0]
                                    winning_status[win_id] = 'W'
                                    winning_status[lose_id] = 'N'
                                    cand_success[win_id] = True
                                    cand_success[lose_id] = False
                                    #print(f"votes_required={votes_required}/{total_votes} {contest_id}:{contest_name} success={conteststat['success']} y/n={candids[0]}:{candids[1]}/{candvotes[0]}:{candvotes[1]}")
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
                        #total_precincts = precinct_count[contest_id]
                        #if args.verbose:
                            #print(f"  precincts ed/mv/nv={ed_precincts}/{mv_precincts}/{nv_precincts} of {total_precincts}")

                        # Split the totallines into a matrix
                        totals = [ line.rstrip().split(separator)
                                  for line in contest_totallines]
                        ntotals = len(totals)
                        if ADD_RESULTS_VECTOR:
                            i = 2 # Starting index for result stats
                            for rsid in resultlist:
                                conteststat['result_stats'].append({
                                    "_id": rsid,
                                    "results":[totals[j][i] for j in range(ntotals)]
                                    })
                                i += 1
                        k = 0
                        cont_winning_status = defaultdict(str)
                        for candid in candids:
                            status = winning_status_names[winning_status.get(candid,'')]
                            if (status != 'rcv_eliminated' and status!='' and
                                status != 'not_winning'):
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
                            s.strip('\n') for s in [headerline] +
                                contest_rcvlines +
                                contest_totallines[:ntotals]]

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
                    addGrandTotal(grand_total,stats)
                    # Append area lines separately
                    contest_arealines.append(outline)

            # End normal data line
        # End Loop over input lines
        if readDictrict or not have_dsov:
            flushcontest(contest_order, contest_id, contest_name,
                        headerline, contest_rcvlines,
                        contest_totallines, contest_arealines)
            putfilea("pctturnout.tsv",
                    "area_id|total_registration|ed_registration|mv_registration|total_ballots|ed_ballots|mv_ballots",
                    pctturnout)
            # Compute per-card turnout
            # TODO
            #for area_id,RSReg  in RSRegSave['TO'].items():
                #VBM_turnout = int(
                #for subtotal_type, ewmfield in [('ED','Polls'),
                                                 #('MV','VBM'),
                                                 #('TO','Total']:
                    #if area_id not in RSRegSave[subtotal_type]: continue
                    #CardsCast = [int(CardTurnOut[i][subtotal_type].get(area_id,0))
                                 #for i in CardTurnOut]
                    #RSCst = max(CardsCast)
                    #RSVot = (EWM_turnout['Grand Total:'].get(ewmfield,'')
                             #if area_id == 'ALL' else
                             #EWM_turnout[area_id[3:].get(ewmfield,'')
                             #if area_id.startswith('PCT') else ''

                    #RSUnc = int(RSVot) - int(RSCst)
                    #if subtotal_type==


        else:
            # Put the json contest status
            #results_json['input_file_sha']=infile_sha
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
            putfile("candlist-sov.tsv",
                    "contest_id|candidate_order|candidate_id|candidate_type|candidate_full_name|candidate_party_id|is_writein_candidate",
                    candlist)

            putfile("pctturnout.tsv",
                    "area_id|total_registration|ed_registration|mv_registration|total_ballots|ed_ballots|mv_ballots",
                    pctturnout)

        # End reading sov.tsv

    # End reading zip file
  # End loop over readDictrict

translator.put_new("unmatched-translations.json")




