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
Program to convert omiballot json ballot definition data

Note: current code assumes a 2 digit ballot type. An upgrade
to scan for the max ballot type to zero fill would be useful
(or else convert to a 3 digit BT.
"""

import argparse
import copy
import json
import os
import os.path
import re
import html

import nameutil

from omniutil import(form_bt_suffix)
from translations import Translator

from config import (Config, config_pattern_list, eval_config_pattern,
                    config_pattern_map, eval_config_pattern_remap,
                    config_strlist_dict, InvalidConfig, config_str_dict,
                    config_pattern_map_dict, eval_config_pattern_map)
from re2 import re2
from tsvio import TSVReader

from datetime import datetime
from collections import OrderedDict
from typing import Union, List, Pattern, Match, Dict, Tuple

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

TURNOUT_LINE_SUFFIX = "Election␤Statement of the Vote"
TRANSLATIONS_FILE = (os.path.dirname(__file__)+
                     "/../submodules/osv-translations/translations.json")

OUT_DIR = "../out-orr"

# Skip title capitalizing upper case for these languages
dont_uncapitalize_lang = {'zh'}
lang_other = ['es','tl','zh']

# Config file handling -----------------------

def config_runoff(d:Dict)->List[Tuple]:
    """
    Validates the "runoff" config that converts:
       date:
         type:
           - list of title patterns
    into a list of:
      (regex, date, type)
    """
    if d is None:
        return []
    runoff_pats = []
    if not isinstance(d,dict):
        raise InvalidConfig(f"Invalid Runoff Config")
    for date, dt in d.items():
        date = str(date)
        if not (re.match(r'^20\d\d-\d\d-\d\d$', date) and
                isinstance(dt,dict)):
            raise InvalidConfig(f"Invalid Runoff Config")
        for runoff_type, l in dt.items():
            if not (re.match(r'(top_2|non_majority)$',runoff_type) and
                    isinstance(l,list)):
                raise InvalidConfig(f"Invalid Runoff Config")
            regex = re.compile(f"({'|'.join(l)})")
            runoff_pats.append((regex, date, runoff_type))
    if args.verbose:
        print("config_runoff=",runoff_pats)
    return runoff_pats



CONFIG_FILE = "config-omni.yaml"
config_attrs = {
    "trim_sequence_prefix": str,            # prefix to chop
    "retention_pats": config_pattern_list,  # Match retention candidate names
    "contest_party_pats": config_pattern_list,  # Match contests with party
    "contest_party_crossover_pats": config_pattern_list,  # Match contests with party
    "bt_digits": int,
    "contest_map_file": str,
    "candidate_map_file": str,
    "approval_required": config_strlist_dict,
    "short_description": config_str_dict,
    "runoff": config_runoff,
    "contest_name_corrections": config_pattern_map_dict,
    "url_state_results_map": config_pattern_map,
    "url_state_results": str,
    "election_voting_district": str,
    "election_base_suffix": str,
    "cand_extid_prefix": bool,
    "turnout_result_style": str,
    }

config_default = {
    "bt_digits": 3,
    "turnout_result_style": "EMTE",
    "election_voting_district": "0",
    "cand_extid_prefix": True,
    "election_base_suffix": ""
    }

APPROVAL_REQUIRED_PAT = re.compile('^(Majority|\d/\d|\d\d%)$')

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

Updates for 2019-11-05:
    "election": {
        "account_id": "06075",
        "admin_title": "2019-11-05 Municipal Election",
        "close_date": 1572940800000,
        "created_at": "2019-08-22T15:43:20Z",
        "external_id": "35bd89db-04a7-4a1f-80af-78a4189db7d4",
        "id": 1485,
        "lang": "",
        "ocd_id": "",
        "open_date": 1568962800000,
        "parent_id": 0,
        "party_handling": 0,
        "party_ids": null,
        "status": "",
        "title": {
            "format": "text",
            "style": "default",
            "translations": {
                "es": "Elecciones Municipales Consolidadas",
                "tl": "Pinagsamang Pangmunisipal na Eleksyon",
                "zh-hant": "聯合市政選舉"
            },
            "value": "Consolidated Municipal Election"
        },
        "type": "",
        "updated_at": "2019-09-18T18:42:00Z",
        "uses_pct_pdfs": false,
        "uses_rotation": true,
        "ver": 35


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
For 2018-11:
        {
            "external_id": "667-rcheader",
            "num_selections": 0,
            "searchable": "ASSESSOR-RECORDER",
            "text": [
                    "value": "<p>You may rank up to three choices. To rank fewer than three candidates, leave any remaining choices blank.</p>"
            "titles": [
                    "value": "ASSESSOR-RECORDER"
            "type": "header",
            "ver": 1
        },
        {
            "external_id": "667",
            "num_selections": 1,
            "titles": [
                    "value": "1. FIRST CHOICE"
            "searchable": "1. FIRST CHOICE,Vote for One",
            "type": "contest",
For 2019-11:
        {
            "admin_title": "Mayor - Title",
            "content_hash": "83782638a8305edecadd22dd544f0e608e327db5",
            "created_at": "0001-01-01T00:00:00Z",
            "district_ids": null,
            "election_id": 1485,
            "external_id": "2a614549-e8df-4827-8a29-7c5f4d098d8e",
            "num_selections": 1,
            "searchable": "MAYOR,Rank up to 6 candidates",
            "sequence": 1,
            "text": [],
            "text_after": [],
            "titles": [
                   "value": "MAYOR"
                    "value": "Rank up to 6 candidates"

            "type": "header",
            "ver": 7
        },
        {
            "account_id": "06075",
            "admin_title": "MAYOR - 1",
            "election_id": 1485,
            "external_id": "334",
            "num_selections": 1,
            "searchable": "1st Choice",
            "sequence": 10,
            "titles": [
                    "value": "1st Choice"
            "type": "contest",
            "ver": 5

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

def decode_JSON_String(s:str)->str:
    """
    Converts HTML encoding within the string back into UTF.
    This might be upgraded later to accomodate included HTML tags so would
    preserve &amp; &gt; &lt;.
    """
    return html.unescape(s)

def get_dict_entry(node, keylist:List[str]):
    """
    If the node is a dict and the has the key, return it's value else None.
    Supports a nested search of keys.
    """
    for key in keylist:
        if not isinstance(node,dict):
            return None
        node = node.get(key,None)
    return node

def form_i18n_str(
        node:Dict,  # Omniballot node
        translate:bool=False    # Include translation lookup
        )->Dict:
    # Change zh-haunt unto just zh
    en = node['value'].strip()
    translations = node['translations']
    zh = translations.pop('zh-hant',None)
    if zh:
        if zh.startswith(en+' '):
            # The zh includes english
            zh = zh[len(en)+1:]
        translations['zh'] = zh
    for lang,v in translations.items():
         foundlang.add(lang)
         translations[lang] = decode_JSON_String(v)

    return {"en":decode_JSON_String(en), **translations }




def join_i18n_str(
        istr:Dict,          # i18n string to be modified
        astr:Dict,          # i18n string to be appended
        sep=' '             # Separator string
        ):
    """
    Concatenates the string with matching language with separator. If
    there is no string, the
    """
    for lang in istr.keys():
        if not lang in astr or not astr[lang]: continue
        # Append astr if present
        if istr[lang]: istr[lang] += sep
        istr[lang] += astr[lang]

    for lang in astr.keys():
        if lang in istr: continue
        # Copy astr if not in istr
        istr[lang] = astr[lang]

def append_i18n_str(
        istr:Dict,      # i18n string to be modified
        s:str           # string to be appended
        ):
    """
    Appends a string to each language
    """
    for lang in istr.keys():
        istr[lang] += s

def fill_i18n_str(
        istr:Dict       # i18n string to be modified
        ):
    """
    Copies the en entry for all other languages as a default
    """
    en = istr['en']
    for lang in lang_other:
        if lang not in istr or not istr[lang]:
            istr[lang] = en

def str2istr(s:str)->Dict:
    return {"en":s}

def conv_titles(titles):
    """
    Extract the titles, and convert to a list of hash values with translations
    """
    return [form_i18n_str(t) for t in titles if t.get('value',"")!=""]

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
        if p['en'].count("<strong") > 1:
            return [{'en':''}]

    for p in paragraphs:
        istr = {}
        for lang in p.keys():
            # Callback for replacement match
            def append_sub(m):
                # Save the extracted title
                istr[lang] = m.group(1)
                return ''
            p[lang] = re.sub(r'^(?:<p\b[^<>]*>)?\s*<strong>\s*(.*?)\s*</strong>\s*(?:</p>|<br \s*/?>)',
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
    election_title_en = election_title['en']
    # In 2019 the admin_title has the date, before the date was a prefix on election_title
    election_admin_title = j['election'].get('admin_title',"")
    m = re.match(r'(\d{4}-\d\d-\d\d) ',election_admin_title)
    if not m:
        m = re.match(r'(\d{4}-\d\d-\d\d) ',election_title_en)
    if m:
        # 2019 format where the date prefix is in admin_title only
        date_prefix = m.group(1)
    else:
        election_title_en = re.sub(r'(\d+)(st|nd|rd|th), (20\d\d)',r'\1, \3',election_title_en)
        m = re.match(r'^(.*?)[ \- ,]+((:?January|February|March|April|May|June|July|August|September|October|November|December) +(\d+), +(20\d\d))\b',
                  election_title_en)
        if m:
            # 2020 format, month day, year format
            date_prefix=datetime.strptime(m.group(2),"%B %d, %Y").strftime('%Y-%m-%d')
            election_title['en'] = m.group(1)
            #print(f"election_title_en={election_title['en']} admin={election_admin_title}")
        else:
            raise FormatError(f"Unmatched election date in '{election_title_en}' admin={election_admin_title}")


    if election_date:
        if date_prefix != election_date:
            raise FormatError(f"Inconsistent election date {election_title} admin={election_admin_title}")
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

        if not titles or not len(titles):
            # Titles can be in <strong> header prefix
            titles = extract_titles(paragraphs)

        clean_paragraphs(paragraphs)

        # Save title and text for RCV, ignore choices after first
        if lastrcvtitle:
            titles = lastrcvtitle
            paragraphs = lastrcvtext
            lastrcvtitle = lastrcvtext = None
            isrcv = True
        elif (external_id.endswith('-rcheader') or
              (len(paragraphs)==1 and
               paragraphs[0].get('en',"").startswith('You may rank up to')) or
              (len(titles)==2 and
               titles[1].get('en',"").startswith('Rank up to'))):
            # Save the title and heading for contest that follows
            lastrcvtitle = titles
            lastrcvtext = paragraphs
            continue
        elif (re.match(r'\d+-\d$',external_id) or (titles and len(titles)>0 and
              re.match(r'\d+(st|nd|rd|th) Choice',titles[0].get('en',"")))):
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

        if isrcv and vote_for == "":
            #print(f"RCV title: {contest_id}|{sequence}|{merge_titles(titles)}|{merge_titles(paragraphs)}")
            if len(paragraphs)==1:
                vote_for_istr = paragraphs[0]
                paragraphs = []
            elif len(titles)==2:
                vote_for_istr = titles[1]
                del titles[1]

            m = re.search(r'up to (\w+) (?:choices|candidates)', vote_for_istr['en'])
            if m:
                vote_for = m.group(1) if m.group(1).isnumeric() else word2num[m.group(1).lower()]
            else:
                print(f"Failed to match number in {vote_for_istr['en']}")
                vote_for = ""
            if len(titles)==2 and titles[1]['en'].startswith('Vote your first,'):
                del titles[1]

        title = merge_titles(titles)
        text = merge_titles(paragraphs)

        if title=='' and len(text)<20:
            print(f"Strange title: {contest_id}|{sequence}|{paragraphs}")


        if len(paragraphs)>1:
            raise FormatError(f"Multiple text paragraphs for {sequence}:{title}")

        mapped_id = mapContestID(external_id)

        if contest_type != "header" and contest_type != "text":
            # Append contest ID to contlist
            contlist.append(mapped_id)

        if found:
            # Save info found in other ballot types
            if contest_type == "header" or contest_type == "text":
                lastheader = sequence
            approval_required = approval_required_by_contest.get(contest_id,'')
        else:
            approval_required = ''
            if contest_type == "header" or contest_type == "text":
                # Process headers separately
                lastheader = sequence
                classification = ("Instructions" if contest_type == "text" else
                                    "Office Category")
                header_id = ""
                hj = headerjson[sequence] = {
                    "_id": sequence,
                    "classification": classification,
                    "header_id": header_id }
                if title and (len(text)<len(title) or text.find(title)!=0):
                    hj["ballot_title"] = translator.check(titles[0])
                if paragraphs:
                    hj["heading_text"] = paragraphs[0]

                # End process header
            else:
                # Must be a contest
                # TODO: Recall
                _type = ("measure" if contest_type == "question" else
                        "office" if contest_type == "contest" else
                        contest_type) # retention

                has_question =  (_type == 'question' or _type == 'retention'
                                 or contest_type == "question")

                # TODO: lookup voting district and compute result style

                # Skip contest with no choices (canceled)
                # TODO: include with type canceled
                if not "options" in box:
                    continue

                # Form a unique name.
                meta_title = get_dict_entry(box,['meta','import','title'])
                if _type == "measure" and meta_title:
                    name = form_i18n_str(meta_title)
                else:
                    name = copy.deepcopy(titles[0])
                    if _type == "measure":
                        # Measure letter/number is en only
                        fill_i18n_str(name)

                    if len(titles)>1:
                        join_i18n_str(name, titles[1])

                # Fix the capitalization and clean titles
                for lang in name.keys():
                    if not lang in dont_uncapitalize_lang:
                        name[lang] = nameutil.uc2_title_case(name[lang])
                    if (config.contest_name_corrections and
                        lang in config.contest_name_corrections):
                        (name[lang], count) = eval_config_pattern_map(name[lang],
                              config.contest_name_corrections[lang])

                contj = contestjson[sequence] = {
                    "_id": mapped_id,
                    "_id_ext": external_id,
                    "_type": _type,
                    "header_id": lastheader,
                    "ballot_title": translator.check(titles[0]),
                    "name": name}

                contest_name = name['en']
                if (config.short_description and
                    contest_name in config.short_description):
                    contj['short_description'] = str2istr(
                        config.short_description[contest_name])

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


                if has_question:
                    approval_required = approval_required_by_title.get(
                        title,"Majority")
                    #print(f"approval_required[{title}]={approval_required}")
                    contj['approval_required'] = approval_required
                    approval_required_by_contest[contest_id] = approval_required
                elif config.runoff:
                    for regex, runoff_date, runoff_type in config.runoff:
                        if regex.search(title):
                            contj['runoff_date'] = runoff_date
                            contj['runoff_type'] = runoff_type
                            newtsvline(runofflines, runoff_date, runoff_type,
                                    mapped_id, external_id, title)

                            break
                contj['choices'] = []

        if contest_type == "retention":
            m = eval_config_pattern(text, config.retention_pats)
            if m:
                contest_candidate = nameutil.uc2_name_case(m.group(1))
                if not found:
                    contj['contest_candidate'] = contest_candidate
                    append_i18n_str(contj['name'],', '+contest_candidate)

            else:
                raise FormatError(f"Mismatched retention candidate in {title}:{text}")
        else:
            contest_candidate = ""

        title_party = title
        m = eval_config_pattern(title, config.contest_party_pats)
        if (m and
            "options" in box and
            "parties" in box["options"][0]):
            party_rec = box["options"][0]["parties"][0]
            contest_party_id = party_rec.get('external_id',"")
            title_party += ':'+contest_party_id
            contest_party_crossover = bool(
                eval_config_pattern(title_party,config.contest_party_crossover_pats))
            if not found:
                contj['contest_party_id'] = contest_party_id
                contj['contest_party_crossover'] = contest_party_crossover
                newtsvline(partycontestlines, contest_party_id,
                           contest_party_crossover, mapped_id, external_id, title)

        else:
            contest_party_id = ''
            contest_party_crossover = False

        url_state_results = eval_config_pattern_remap(title_party,
                                config.url_state_results_map)
        if url_state_results and not found:
            #print(f"url_state_results {title_party}={url_state_results}")
            contj['url_state_results'] = url_state_results

        l = jointsvline(sequence,contest_type,contest_id,external_id,vote_for,title,text,
                        approval_required,contest_party_id)
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
                if config.cand_extid_prefix:
                    cand_external_id = sequence+cand_external_id

                cand_mapped_id = mapCandID(cand_external_id)
                #if mapped_id == "342":
                    #print(f"id={cand_id}:{cand_external_id}:{cand_mapped_id}")
                candids.append(cand_external_id)
                cand_seq = str(o["sequence"]).zfill(3)
                cand_mapped_seq = candseq.get(cand_external_id,cand_seq)
                cand_names = conv_titles(o["titles"])
                designation = party = ""
                designation_istr = party_istr = None
                if len(cand_names)>1:
                    designation_istr = cand_names.pop()
                    designation = designation_istr['en']
                    if len(cand_names)>1:
                        party_istr = cand_names.pop()
                        party = party_istr['en']
                    if designation.startswith('Party Preference:'):
                        tmp_des = party
                        tmp_des_istr = party_istr
                        party_istr = designation_istr
                        party = designation
                        designation = tmp_des
                        designation_istr = tmp_des_istr
                if len(cand_names)!=1:
                    raise FormatError("Strange candidate {cand_id} in {contest_id}:{title}")

                cand_name = cand_names[0]['en']
                party = re.sub(r'^Party Preference:\s*','', party)

                if int(cand_seq) <= int(lastseq):
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
                        "sequence": cand_mapped_seq
                        }
                    if party_istr:
                        candj['candidate_party'] = party_istr
                    if designation_istr:
                        candj['ballot_designation'] = designation_istr
                    contj['choices'].append(candj)
            # End loop over contest choices
            # Sort by sequence
            if not found and "options" in box:
                contj['choices'] = sorted(contj['choices'],
                                      key=lambda c:c['sequence'])
            # Insert manually added candidates
            if not found and mapped_id in added_contcand:
                print(f"external_id={mapped_id} in added_contcand")
                for candj in added_contcand[mapped_id]:
                    contj['choices'].append(candj)
                    candj['sequence']=len(contj['choices'])

            candlist = ' '.join(candids)
            if not found and contest_type != "header" and contest_type != "text":
                if _type == "office":
                    contj['writeins_allowed'] = writein_lines
                contj['voting_district'] = (distmap[external_id]
                                if external_id in distmap else
                                config.election_voting_district)
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
            # End Contest with a list of choices
        elif not found and contest_type != "header" and contest_type != "text":
            # Contest with no choices
            contj['result_style'] = 'EMS'
            contj['_type'] = "canceled"

    # End loop over boxes
    # Put the contests found for this bt
    newtsvline(btcontlines, bt, ' '.join(contlist))

args = parse_args()

config = Config(CONFIG_FILE, valid_attrs=config_attrs, default_config=config_default)

json_dump_args = PP_JSON_DUMP_ARGS if args.pretty else DEFAULT_JSON_DUMP_ARGS

separator = "|" if args.pipe else "\t"

# Load translations
translator = Translator(TRANSLATIONS_FILE)

contestlines = []   # Extracted contest definition lines
candlines = []      # Extracted candidate definition lines
btcontlines = []    # List of contest IDs for each ballot type
btpctlines = []     # List of (consolidated) precincts by bt
runofflines = []    # List of contests with a runoff
partycontestlines = [] # List of party-only contests

candorder = {}      # List of candidate rotations by ballot type
foundContest = {}   # Prior contest definition line
contid2seq = {}     # Converts contest external ID to sequence

contestjson = {}    # JSON for contest by sequence
headerjson = {}     # JSON for header by sequence
lastheader = []

foundlang = { "en" }

ballot_title = None
election_date = None

approval_required_by_title = {}  # Approval required by measure title
approval_required_by_contest = {}


# Invert the approval_required to make a lookup
# Default will be "Majority"
if config.approval_required:
    for (approval, l) in config.approval_required.items():
        if not APPROVAL_REQUIRED_PAT.match(approval):
            raise InvalidConfig(f"Invalid Approval Required '{approval}'")
        for i in l:
            approval_required_by_title[i] = approval
#print("Approval req:",approval_required_by_title)


# read the list of styles and precincts in LOOKUPS_URL saved to file lookups.json

'''
Notes on the format of lookups.json
styles[id].code = ballot type (internal/wrong)
           .name = "Poll BT 9", (correct in name - prior year matched code)
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

def mapContestID(contest_id:str # contest ID to map
                 )->str:  # Returns
    mapped_id = contmap.get(contest_id,"")
    return mapped_id if mapped_id != "" else contest_id

def mapCandID(cand_id:str # candidate ID to map
                 )->str:  # Returns
    mapped_id = candmap.get(cand_id,"")
    return mapped_id if mapped_id != "" else cand_id

with open("lookups.json") as f:
    lookups = json.load(f)

# Construct the internal precinct to external precinct ID map
pct2extid = {}
for p in lookups['precincts'].values():
    #Concatenate precinct and split
    pct2extid[p['id']] = p['pid'] + pctsplitsep + p.get('sid','')

# Check for a contest map
if config.contest_map_file and os.path.isfile(config.contest_map_file) :
    have_contmap = True
    with TSVReader(config.contest_map_file) as r:
        contmap = r.load_simple_dict(0,1)
else:
    have_contmap = False
    contmap = {}

# Retrieve short contest name from sov data
# Disabled
#contid2name = {}
#if os.path.isfile("../resultdata/contlist-sov.tsv"):
    #with TSVReader("../resultdata/contlist-sov.tsv") as r:
       #contid2name = r.load_simple_dict(1,3)

if os.path.isfile("distmap.ems.tsv"):
    with TSVReader("distmap.ems.tsv") as r:
        distmap = r.load_simple_dict(0,2)
else:
    distmap = {}

# Check for a candidate map
candmap = {}
candseq = {}
if config.candidate_map_file:
    have_candmap = True
    candmap_header = "contest_id2|cand_id2|contest_id1|cand_id1|cand_seq|"\
        "cand_name2|cand_name1"
    with TSVReader(config.candidate_map_file) as r:
        if r.headerline != candmap_header:
            raiseFormatError(f"Unmatched {config.candidate_map_file} header {r.headerline}")
        for cols in r.readlines():
            candmap[cols[1]] = cols[3]
            candseq[cols[1]] = cols[4]
else:
    have_candmap = False

#print(f"candmap={candmap}")

#Check for added candidates (including write-in)
added_contcand = {}
writein_candlines = []

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
            mapped_id = mapContestID(cont_external_id)
            cand_mapped_id = mapCandID(external_id)
            candj = {
                "_id": cand_mapped_id,
                "_id_ext": external_id,
                "ballot_title": str2istr(title),
                }
            if party:
                candj['candidate_party'] = str2istr(party)
            if designation:
                candj['ballot_designation'] = str2istr(designation)
            if cand_type == "writein":
                candj['is_writein'] = True
            if mapped_id  not in added_contcand:
                added_contcand[mapped_id] = []
            added_contcand[mapped_id].append(candj)

writeinfiles = [
    "../resultdata/CandidateManifest.tsv",
    "../resultdata/candlist-sov.tsv",
    ]
writein_header = [
    "Description|Id|ExternalId|ContestId|Type",
    "contest_id|candidate_order|candidate_id|candidate_type"\
        "|candidate_full_name|candidate_party_id|is_writein_candidate",
    ]
for wiformat in range(1,len(writeinfiles)):
    filename = writeinfiles[wiformat]
    if not os.path.isfile(filename): continue
# Load QualifiedWritin from CVR manifest
    with TSVReader(filename,
        validate_header=writein_header[wiformat]) as r:
        seq=900
        for cols in r.readlines():
            if wiformat==0:
                # Load QualifiedWritin from CVR manifest
                Description, Id, ExternalId, ContestId, Type = cols
                if Type != "QualifiedWriteIn":
                    continue
            else:
                # Extract from the SOV XLS
                (ContestId, candidate_order, Id, candidate_type, Description,
                 candidate_party_id, is_writein_candidate) = cols
                ExternalId = 'WI'+Id
                if is_writein_candidate != 'Y':
                    continue;
            seq += 1
            if ContestId not in added_contcand:
                added_contcand[ContestId] = []
            newtsvline(writein_candlines,ContestId,seq,Id, Description)
            added_contcand[ContestId].append({
                "_id": Id,
                "_id_ext": ExternalId,
                "ballot_title": str2istr(Description),
                'is_writein': True
                })
    break

if writein_candlines:
    putfile("candlist-writein.tsv",
        "contest_id|candidate_order|candidate_id|candidate_full_name",
        writein_candlines)


# Read the composite ballot
try:
    with open("bt/btcomposite.json") as f:
        j = json.load(f)
        conv_bt_json(j, "000")
except:
    print("cannot process bt/btcomposite.json")

os.makedirs("bt", exist_ok=True)

for sid, s in lookups['styles'].items():
    bt = form_bt_suffix(s)
    print(f'Reading Style {sid} for bt {bt} ({s["code"]}/{s["name"]})')
    with open(f'bt/bt{bt}.json') as f:
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
        "sequence|type|contest_id|external_id|vote_for|title|text|approval_required|contest_party_id",
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

putfile("runoff-omni.tsv",
        "runoff_date|runoff_type|contest_id|external_id|title",
        runofflines)

putfile("party-contests.tsv",
        "party_id|crossover|contest_id|external_id|title",
        partycontestlines)

# Build the area list
arealist = [{
    "_id": "ALLPCTS",
    "classification": "All",
    "name": "All Precincts",
    "short_name": "All Precincts"
        }]

no_voter_precincts = []

pctconsfile = None
for path in ["../ems/pctcons.tsv","../resultdata/pctcons-sov.tsv"]:
    if os.path.isfile(path):
        pctconsfile = path
if not pctconsfile:
    print("Missing ../ems/pctcons.tsv or ../resultdata/pctcons-sov.tsv")
else:
    with TSVReader(path) as r:
        if r.headerline != "cons_precinct_id|cons_precinct_name|is_vbm|no_voters"\
        "|precinct_ids":
            raiseFormatError(f"Unmatched pctcons.tsv header {r.headerline}")
        for cols in r.readlines():
            (cons_precinct_id, cons_precinct_name, is_vbm, no_voters,
            precinct_ids) = cols
            area = {
                "_id": "PCT"+cons_precinct_id,
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

# Load the result groups by district
if os.path.isfile("reporting_groups.tsv"):
    with TSVReader("reporting_groups.tsv") as r:
        reporting_groups_by_distid = r.load_simple_dict(0,1)
else:
    reporting_groups_by_distid = {}
    print("Run extresultdist.py to extract reporting groups")

with TSVReader("../ems/distclass.tsv") as r:
    if r.headerline != "District_Code|Classification|District_Name|District_Short_Name":
        raiseFormatError(f"Unmatched distnames.tsv header {r.headerline}")
    for cols in r.readlines():
        (District_Code, Classification, District_Name, District_Short_Name) = cols

        # TODO: Compute classification
        arealist.append({
            "_id": District_Code,
            "name": District_Name,
            "short_name": District_Short_Name,
            "classification": Classification,
            "reporting_group_ids": reporting_groups_by_distid.get(District_Code,"")
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
        "ballot_title": translator.check(ballot_title),
        "headers": [ headerjson[i] for i in sorted(headerjson) ],
        "contests": [ contestjson[i] for i in sorted(contestjson) ],
        "turnout": {
            "_id": "TURNOUT",
            "_type": "turnout",
            "ballot_title": translator["registration_and_turnout_header"],
            "voting_district": config.election_voting_district,
            "result_style": config.turnout_result_style
        },
        "no_voter_precincts": no_voter_precincts,
    },
    "languages": sorted(foundlang),
    "areas": arealist
    }

if config.url_state_results:
    outj["election"]["url_state_results"] = config.url_state_results

# For now use a fixed path
path = os.path.dirname(__file__)+f"/../json/election-base{config.election_base_suffix}.json"
with open(path) as f:
    basejson = json.load(f)

outj.update(basejson)

if not os.path.exists(OUT_DIR):
    os.makedirs(OUT_DIR)

with open(f"{OUT_DIR}/election.json",'w') as outfile:
    json.dump(outj, outfile, **json_dump_args)
    outfile.write("\n")

translator.print_unmatched("unmatched-translations.txt")
