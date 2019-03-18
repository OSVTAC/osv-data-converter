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
Program to convert omiballot json ballot definition data

Note: current code assumes a 2 digit ballot type. An upgrade
to scan for the max ballot type to zero fill would be useful
(or else convert to a 3 digit BT.
"""

import argparse
import json
import os
import os.path
import re

from config import Config, config_pattern_list, eval_config_pattern
from tsvio import TSVReader
from re2 import re2

from datetime import datetime
from collections import OrderedDict
from typing import Union, List, Pattern, Match, Dict

DESCRIPTION = """\
Convert data in omniballot sample ballots.

Reads the files:
  * lookups.json
  * bt/btnn.json

Creates the files:
  * election.json

"""

VERSION='0.0.1'     # Program version

SF_ENCODING = 'ISO-8859-1'
SF_HTML_ENCODING = 'UTF-8'

OUT_DIR = "../out-orr"

CONFIG_FILE = "config-omni.yaml"
config_attrs = {
    "trim_sequence_prefix": str,            # prefix to chop
    "retention_pats": config_pattern_list,  # Match retention candidate names
    "bt_digits": int,
    "contest_map_file": str,
    "candidate_map_file": str
    }

DEFAULT_JSON_DUMP_ARGS = dict(sort_keys=False, separators=(',\n',':'), ensure_ascii=False)
PP_JSON_DUMP_ARGS = dict(sort_keys=False, indent=4, ensure_ascii=False)

class FormatError(Exception):pass # Error matching expected input format

word2num = dict((v,i) for (i,v) in enumerate(
    "zero one two three four five six seven eight nine ten eleven twelve thirteen".split()))

# These can be config items:
pctsplitsep = ''    # Optional precinct split separator

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

    args = parser.parse_args()

    return args

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
    datalist.append(jointsvline(*args))

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

def put_json_file(j, filename):
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(j, f, skipkeys=True, ensure_ascii=False)

'''
Notes on the omniballot json (per ballot type or composite):

    "account_id": "06075",  # FIPS state-county ID
     "election_id": 463,
    "external_id": "6252",
    "election": {
        "external_id": "864",
        "id": 463,
        "title": {
            "format": "text",
            "style": "default",
            "value": "2018-06-05 Consolidated Statewide Direct Primary Election"
            "translations": {
                "es": "2018-06-05 Elecciones Estatales Primarias Directas Consolidadas",

    "precincts": [
            "external_id": "8398",  # Unknown ID
            "id": 79649,            # Internal ID
            "pid": "1101",          # Consolidated precinct ID
            "pname": "Pct 1101",    # Name
    "boxes": [              # List of instructions and headings
            "id": 57411,
            "sequence": 1,
            "external_id": "intro",
            "type": "text",
            "meta": {
                "import": {
                    "text": {
                        "format": "html",
                        "translations": { # Non-english
                            "es": "<p><strong>Cargos Nominados..
                         "value": "<p><strong>Voter-Nominated ...
                    "title": {
                        "translations": {},
                        "value": "Voter-Nominated and Nonpartisan Offices"
                "styles": 36,
                "templateId": "Default Text"
            "titles": [],
            "text": [
                    "format": "html",
                    "style": "default",
                    "translations": {
                        "es": "<p><strong>Cargos Nominados ...
                    },
                    "value": "<p><strong>Voter-Nominated ...

            "external_id": "header-1",
            "id": 57399,
            "sequence": 97,
            "type": "header",
            "text": [],
            "meta": {
                "templateId": "Default Header"
            "titles": [
                    "format": "style",
                    "style": "default",
                    "translations": {
                        "es": "CARGOS NOMINADOS POR LOS ELECTORES",
                        "tl": "MGA KATUNGKULANG NOMINADO NG BOTANTE",
                        "zh-hant": "選民提名職位"
                    },
                    "value": "VOTER-NOMINATED OFFICES"

            "type": "contest",
            "titles": [
                    "format": "style",
                    "style": "default",
                    "value": "GOVERNOR"
                    "style": "subtitle",
                    "value": "Vote for One"
            "options": [
                    "external_id": "4862",
                    "id": 27721,
                    "sequence": 10,
                    "titles": [
                        {
                            "format": "style",
                            "style": "default",
                            "translations": {
                                "zh-hant": "約翰 H. 考克斯"
                            },
                            "value": "JOHN H. COX"
                            "format": "style",
                            "style": "subtitle",
                            "value": "Party Preference: Republican"
                            "translations": {
                                "es": "Preferencia por partido: Republicano",
                            },
                            "format": "style",
                            "style": "subtitle",
                            "translations": {
                                "es": "Empresario / Defensor del Contribuyente",
                                "tl": "Negosyante / Tagapagtaguyod ng Nagbabayad ng Buwis",
                                "zh-hant": "企業家 / 納稅人維權者"
                            },
                            "value": "Businessman / Taxpayer Advocate"
'''

def form_i18n_str(node:Dict)->Dict:
    # Change zh-haunt unto just zh
    zh = node['translations'].pop('zh-hant',None)
    if zh:
        node['translations']['zh'] = zh
    for lang in node['translations'].keys():
         foundlang.add(lang)
    return {"en":node['value'].strip(), **node['translations'] }

def str2istr(s:str)->Dict:
    return {"en":s}

def conv_titles(titles):
    """
    Extract the titles, and convert to a list of hash values with translations
    """

    return [form_i18n_str(t) for t in titles ]

def merge_titles(titles:List[Dict])->str:
    """
    Merge english with ~ separator
    """
    return '~'.join([t['en'] for t in titles])

def clean_paragraphs(paragraphs:List[Dict]):
    """
    Remove superflous <p>..</p> wrapper
    """
    for p in paragraphs:
        for lang in p.keys():
            if p[lang].count("<p") > 1:
                continue
            p[lang] = re.sub(r'^\s*<p\b[^<>]*>\s*(.*?)\s*</p>\s*$',r"\1", p[lang])

def extract_titles(paragraphs:List[Dict])->List[Dict]:
    """
    Reforms a set of titles from <strong> wrapped titles in paragraphs.
    Moves the heading from the paragraph to returned titles.
    """
    titles = []

    for p in paragraphs:
        istr = {}
        for lang in p.keys():
            # Callback for replacement match
            def append_sub(m):
                # Save the extracted title
                istr[lang] = m.group(1)
                return ''
            p[lang] = re.sub(r'^<strong>(.*?)</strong><br ?/?>',
                                append_sub, p[lang], count=1)
        if istr:
            titles.append(istr)
    return titles

def conv_bt_json(j:Dict, bt:str):
    """
    Read the omniballot json and extract headings and contests
    """
    global election_date, ballot_title
    global config

    lastrcvtitle = lastrcvtext = None
    lastheader = ""

    election_title = form_i18n_str(j['election']['title'])
    date_prefix = election_title['en'][:11]
    # Trim election date prefix
    for lang in election_title.keys():
        if not election_title[lang].startswith(date_prefix):
            raise FormatError(f"Inconsistent election date prefix {election_titles[0][lang]}")
        election_title[lang] = election_title[lang][11:]
    date_prefix = date_prefix[:-1]
    if election_date:
        if date_prefix != election_date:
            FormatError(f"Inconsistent election date")
    else:
        election_date = date_prefix
        ballot_title = election_title

    contlist = []

    for box in j["boxes"]:
        contest_id = str(box["id"])
        found = contest_id in foundContest
        sequence = str(box["sequence"]).zfill(3)
        if config.trim_sequence_prefix:
            # RCV reordering hack by adding the prefix 9
            if len(sequence)>3 and sequence.startswith(config.trim_sequence_prefix):
                sequence = sequence[len(config.trim_sequence_prefix):]
        external_id = str(box["external_id"])
        contid2seq[external_id] = sequence
        contest_type = box['type']
        titles = conv_titles(box['titles'])
        paragraphs = conv_titles(box['text'])
        clean_paragraphs(paragraphs)

        if not titles:
            # Titles can be in <strong> header prefix
            titles = extract_titles(paragraphs)

        # Save title and text for RCV, ignore choices after first
        if lastrcvtitle:
            titles = lastrcvtitle
            paragraphs = lastrcvtext
            lastrcvtitle = lastrcvtext = None
            isrcv = True
        elif external_id.endswith('-rcheader'):
            # Save the title and heading for contest that follows
            lastrcvtitle = titles
            lastrcvtext = paragraphs
            continue
        elif re.search(r'-\d$',external_id):
            # 2nd, 3rd RCV choices
            continue
        else:
            isrcv = False

        # Extract Vote for number
        ilast = len(titles)-1
        if ilast>0 and titles[ilast]['en'].startswith('Vote for'):
            vote_for_istr = titles.pop()
            try:
                vote_for = word2num[re.search(r'(\w+)$',
                                     vote_for_istr['en']).group(1).lower()]
            except:
                vote_for = ""
        else:
            vote_for_istr = None
            vote_for = ""

        title = merge_titles(titles)
        text = merge_titles(paragraphs)

        if title=='':
            print(f"Strange title: {contest_id}|{sequence}|{paragraphs}")

        if isrcv and vote_for == "" and len(paragraphs)==1:
            vote_for_istr = paragraphs[0]
            paragraphs = []
            m = re.search(r'up to (\w+) choices', vote_for_istr['en'])
            if m:
                vote_for = word2num[m.group(1).lower()]
            else:
                print(f"Failed to match number in {vote_for_istr['en']}")
                vote_for = ""


        if len(paragraphs)>1:
            raise FormatError(f"Multiple text paragraphs for {sequence}:{title}")

        if external_id in contmap:
            mapped_id = contmap[external_id]
        else:
            mapped_id = external_id

        if contest_type != "header" and contest_type != "text":
            # Append contest ID to contlist
            contlist.append(mapped_id)

        if not found:
            if contest_type == "header" or contest_type == "text":
                # Process headers separately
                lastheader = sequence
                classification = ("Instructions" if contest_type == "text" else
                                    "Office Category")
                header_id = ""
                hj = headerjson[sequence] = {
                    "_id": sequence,
                    "classification": classification,
                    "header_id": header_id,
                    "ballot_title": titles }
                if paragraphs:
                    hj["heading_text"] = paragraphs[0]

                # End process header
            else:
                # Must be a contest
                _type = ("measure" if contest_type == "question" else
                        "office" if contest_type == "contest" else
                        contest_type) # retention

                has_question =  _type == 'question' or _type == 'retention'

                # TODO: lookup voting district and compute result style

                contj = contestjson[sequence] = {
                    "_id": mapped_id,
                    "_id_ext": external_id,
                    "_type": _type,
                    "header_id": lastheader,
                    "ballot_title": titles[0]}

                if len(titles)>1:
                    contj['ballot_subtitle'] = titles[1]
                if vote_for_istr:
                    contj['vote_for_msg'] = vote_for_istr
                if isrcv:
                    contj['max_ranked'] = vote_for
                    contj['number_elected'] = 1
                elif vote_for and not has_question:
                    contj['number_elected'] = vote_for
                if paragraphs:
                    if contest_type != "question" and contest_type != "retention":
                        raise FormatError(f"Unknown paragraph in {title}:{text}")
                    contj['question_text'] = paragraphs[0]
                contj['choices'] = []


        if contest_type == "retention":
            m = eval_config_pattern(text, config.retention_pats)
            if m:
                contest_candidate = m.group(1)
                if not found:
                    contj['contest_candidate'] = contest_candidate
            else:
                raise FormatError(f"Mismatched retention candidate in {title}:{text}")
        else:
            contest_candidate = ""

        l = jointsvline(sequence,contest_type,contest_id,external_id,vote_for,title,text)
        if found:
            if l != foundContest[contest_id]:
                raise FormatError(f"Mismatched contest:\n  {l}  {foundContest[contest_id]}")
        else:
            contestlines.append(l)
            foundContest[contest_id] = l

        candids = []
        lastseq = "000"

        if "options" in box:
            # Contest with a list of choices
            writein_lines = 0
            for o in box["options"]:
                cand_external_id = o["external_id"]
                if cand_external_id.startswith("writein-"):
                    # Count writeins and skip
                    writein_lines += 1
                    continue
                cand_id = str(o["id"])

                if cand_external_id in candmap:
                    cand_mapped_id = candmap[cand_external_id]
                else:
                    cand_mapped_id = cand_external_id
                candids.append(cand_external_id)
                cand_seq = str(o["sequence"]).zfill(3)
                cand_names = conv_titles(o["titles"])
                designation = party = ""
                designation_istr = party_istr = None
                if len(cand_names)>1:
                    designation_istr = cand_names.pop()
                    designation = designation_istr['en']
                    if len(cand_names)>1:
                        party_istr = cand_names.pop()
                        party = party_istr['en']
                    elif designation.startswith('Party Preference:'):
                        party_istr = designation_istr
                        party = designation
                        designation = ""
                        designation_istr = None
                if len(cand_names)!=1:
                    raise FormatError("Strange candidate {cand_id} in {contest_id}:{title}")

                cand_name = cand_names[0]['en']
                party = re.sub(r'^Party Preference:\s*','', party)

                if cand_seq <= lastseq:
                    raise FormatError(
        f"Out of sequence {contest_id}:{cand_id}:{cand_name} {lastseq}..{cand_seq}")
                cand_type = o['type']
                lastseq = cand_seq
                if not found:
                    newtsvline(candlines, sequence, external_id,
                               cand_seq, cand_id, cand_external_id,
                               cand_name, party, designation, cand_type)
                    candj = {
                        "_id": cand_mapped_id,
                        "_id_ext": cand_external_id,
                        "ballot_title": cand_names[0],
                        }
                    if party_istr:
                        candj['candidate_party'] = party_istr
                    if designation_istr:
                        candj['ballot_designation'] = designation_istr
                    contj['choices'].append(candj)
            # End loop over contest choices
            # Insert manually added candidates
            if not found and external_id in added_contcand:
                print(f"external_id={external_id} in added_contcand")
                contj['choices'].extend(added_contcand[external_id])

            candlist = ' '.join(candids)
            if not found and (_type == "office" or _type == "measure"):
                if _type == "office":
                    contj['writeins_allowed'] = writein_lines
                if isrcv:
                    if writein_lines:
                        contj['result_style'] = 'EMRW'
                    else:
                        contj['result_style'] = 'EMR'
                else:
                    if writein_lines:
                        contj['result_style'] = 'EMSW'
                    else:
                        contj['result_style'] = 'EMS'


            if external_id not in candorder:
                candorder[external_id] = {}
            if candlist not in candorder[external_id]:
                candorder[external_id][candlist] = order = []
            else:
                order = candorder[external_id][candlist]
            order.append(bt)
        # End processing options
    # End loop over boxes
    # Put the contests found for this bt
    newtsvline(btcontlines, bt, ' '.join(contlist))

args = parse_args()

config = Config(CONFIG_FILE, valid_attrs=config_attrs)

json_dump_args = PP_JSON_DUMP_ARGS if args.pretty else DEFAULT_JSON_DUMP_ARGS

separator = "|" if args.pipe else "\t"

contestlines = []   # Extracted contest definition lines
candlines = []      # Extracted candidate definition lines
btcontlines = []    # List of contest IDs for each ballot type
btpctlines = []     # List of (consolidated) precincts by bt

candorder = {}      # List of candidate rotations by ballot type
foundContest = {}   # Prior contest definition line
contid2seq = {}     # Converts contest external ID to sequence

contestjson = {}    # JSON for contest by sequence
headerjson = {}     # JSON for header by sequence
lastheader = []

foundlang = { "en" }

ballot_title = None
election_date = None



# read the list of styles and precincts in LOOKUPS_URL saved to file lookups.json

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

with open("lookups.json") as f:
    lookups = json.load(f)

# Construct the internal precinct to external precinct ID map
pct2extid = {}
for p in lookups['precincts'].values():
    #Concatenate precinct and split
    pct2extid[p['id']] = p['pid'] + pctsplitsep + p.get('sid','')

# Check for a contest map
if config.contest_map_file:
    have_contmap = True
    with TSVReader(config.contest_map_file) as r:
        contmap = r.load_simple_dict(0,1)
else:
    have_contmap = False
    contmap = {}

# Check for a candidate map
if config.candidate_map_file:
    have_candmap = True
    with TSVReader(config.candidate_map_file) as r:
        candmap = r.load_simple_dict(1,3)
else:
    have_candmap = False
    candmap = {}


#Check for added candidates (including write-in)
added_contcand = {}

if os.path.isfile("candlist-fix.tsv"):
    candlist_fix_header = "sequence|cont_external_id|cand_seq|cand_id|external_id"\
        "|title|party|designation|cand_type"
    with TSVReader("candlist-fix.tsv") as r:
        if r.headerline != candlist_fix_header:
            raiseFormatError(f"Unmatched candlist-fix.tsv header {r.headerline}")
        for cols in r.readlines():
            (sequence, cont_external_id, cand_seq, cand_id, external_id, title,
             party, designation, cand_type) = cols
            newtsvline(candlines, sequence, cont_external_id,
                        cand_seq, cand_id, external_id,
                        title, party, designation, cand_type)
            candj = {
                "_id": external_id,
                "ballot_title": str2istr(title),
                }
            if party:
                candj['candidate_party'] = str2istr(party)
            if designation:
                candj['ballot_designation'] = str2istr(designation)
            if cand_type == "writein":
                candj['is_writein'] = True
            if cont_external_id not in added_contcand:
                added_contcand[cont_external_id] = []
            added_contcand[cont_external_id].append(candj)



# Read the composite ballot
try:
    with open("bt/btcomposite.json") as f:
        j = json.load(f)
        conv_bt_json(j, "000")
except:
    print("cannot process bt/btcomposite.json")

os.makedirs("bt", exist_ok=True)

for sid, s in lookups['styles'].items():
    bt = s["code"].zfill(config.bt_digits)
    if bt == 'composite':
        continue

    print(f'Reading Style {sid} for bt {s["code"]}')
    with open(f"bt/bt{bt}.json") as f:
        j = json.load(f)
        conv_bt_json(j, bt)

    #Append btpct.tsv data
    newtsvline(btpctlines, bt, ' '.join(
        [pct2extid[p] for p in s["precinct_ids"]]))



# End loop over ballot types

# Compute candidate rotation
candrotlines = []

for (contest_id, orderlist) in candorder.items():
    for (candlist, btlist) in orderlist.items():
        seq = contid2seq[contest_id]
        newtsvline(candrotlines, seq, contest_id, candlist, ' '.join(btlist))

putfile("controt-omni.tsv",
        "sequence|cont_external_id|cand_external_ids|bts",
        candrotlines)

# Write out the collected TSV data

putfile("contlist-omni.tsv",
        "sequence|type|contest_id|external_id|vote_for|title|text",
        contestlines)

putfile("candlist-omni.tsv",
        "sequence|cont_external_id|cand_seq|cand_id|external_id|title|party|designation|cand_type",
        candlines)

putfile("btcont-omni.tsv",
        "ballot_type|contest_ids",
        btcontlines)

putfile("btpct-omni.tsv",
        "ballot_type|cons_precinct_ids",
        btpctlines)

# Build the area list
arealist = [{
    "_id": "ALLPCTS",
    "classification": "All",
    "name": "All Precincts",
    "short_name": "All Precincts"
        }]

no_voter_precincts = []

with TSVReader("../ems/pctcons.tsv") as r:
    if r.headerline != "cons_precinct_id|cons_precinct_name|is_vbm|no_voters"\
    "|precinct_ids":
        raiseFormatError(f"Unmatched pctcons.tsv header {r.headerline}")
    for cols in r.readlines():
        (cons_precinct_id, cons_precinct_name, is_vbm, no_voters,
         precinct_ids) = cols
        area = {
            "_id": cons_precinct_id,
            "classification": "Precinct",
            "name": cons_precinct_name,
            "short_name": cons_precinct_name
        }
        if is_vbm=='Y':
            area['is_vbm'] = True
        if no_voters=='Y':
            area['has_no_voters'] = True
            no_voter_precincts.append(cons_precinct_id)
        arealist.append(area)

with TSVReader("../ems/distnames.tsv") as r:
    if r.headerline != "District_Code|District_Name|District_Short_Name":
        raiseFormatError(f"Unmatched distnames.tsv header {r.headerline}")
    for cols in r.readlines():
        (District_Code, District_Name, District_Short_Name) = cols
        # TODO: Compute classification
        arealist.append({
            "_id": District_Code,
            "name": District_Name,
            "short_name": District_Short_Name
        })





# TODO: Compute the election area (reporting area)
election_area = {
"en":	"City and County of San Francisco",
"es":	"Ciudad y Condado de San Francisco",
"tl":	"Lungsod at County ng San Francisco",
"zh":	"舊金山市"}

# Load base json to copy
# Compose the output json
outj = {
    "election": {
        "election_date": election_date,
        "election_area": election_area,
        "ballot_title": ballot_title,
        "headers": [ headerjson[i] for i in sorted(headerjson) ],
        "contests": [ contestjson[i] for i in sorted(contestjson) ]
        },
    "languages": sorted(foundlang),
    "areas": arealist,
    "no_voter_precincts": no_voter_precincts

    }

# For now use a fixed path
path = os.path.dirname(__file__)+"/../json/election-base.json"
with open(path) as f:
    basejson = json.load(f)

outj.update(basejson)



with open(f"{OUT_DIR}/election.json",'w') as outfile:
    json.dump(outj, outfile, **json_dump_args)

