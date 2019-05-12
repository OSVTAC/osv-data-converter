#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to match contest IDs by comparing candidate names:


"""

import logging
import os
import os.path
import re
import argparse
from typing import Dict, Tuple, List, TextIO, Union, Pattern
from tsvio import TSVReader, TSVWriter, DuplicateError
from candutil import CandContMatch
from collections import namedtuple
from config import Config, config_whole_pattern_list, eval_config_pattern, eval_config_pattern_map, config_whole_pattern_map, PatternMap

DESCRIPTION = """\
Creates an external contest ID mapping by matching candidate names by contest.

Reads the following files:
  * master candidate list tsv
  * secondary candidate list tsv to match
  * Optional master contest list tsv
  * Optional secondary contest list tsv

Creates the following files:
  * contmap.tsv
"""
VERSION='0.0.1'     # Program version

DFM_ENCODING = 'ISO-8859-1'
OUT_ENCODING = 'UTF-8'

#CONFIG_FILE = "config-matchcand.yaml"

# namedtuple represents a list of a subrecord type
config_fileinfo = {
        "filename": str,            # tsv input file
        "file_type": str,           # (contest|candidate)[-master]
        "contest_id_col": str,      # contest ID column
        "district_id_col": str,     # district ID column
        "contest_name_col": str,    # contest name column (contest file)
        "contest_text_col": str,    # contest question text column (contest file)
        "contest_type_col": str,    # contest type column (contest file)
        "skip_contest_types": config_whole_pattern_list,  # Regex to match skipped types
        "candidate_id_col": str,    # candidate ID column (candidate_file)
        "candidate_seq_col": str,   # candidate sequence column (candidate_file)
        "candidate_name_col": str,  # candidate name column (candidate_file)
        "question_pats": config_whole_pattern_map   # mapping of question names
        }

config_attrs = {
    "files": [ namedtuple('FileInfo',config_fileinfo.keys())(*config_fileinfo.values()) ],
    "default_district_id": str
    }

def parse_args():
    """
    Parse sys.argv and return a Namespace object.
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION,
                    formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='enable verbose info printout')
    parser.add_argument('-w', '--warn', action='store_true',
                        help='enable verbose warnings')
    parser.add_argument('-D', '--debug', action='store_true',
                        help='enable debug logging')
    parser.add_argument('-S', '--suffix', default="",
                        help='suffix for config and output files')
    parser.add_argument('-p', dest='pipe', action='store_true',
                        help='use pipe separator else tab')

    args = parser.parse_args()

    return args

args = parse_args()

if args.debug:
    logging.basicConfig(level=logging.DEBUG)

candmap_header = "contest_id2|cand_id2|contest_id1|cand_id1|cand_seq|cand_name2|cand_name1"
contmap_header = "contest_id2|contest_id1|contest_name2|contest_name1"
distmap_header = "contest_id2|contest_id1|district_id"

separator = "|" if args.pipe else "\t"


config = Config(f"config-matchcand{args.suffix}.yaml", valid_attrs=config_attrs)

m = CandContMatch(debug=args.debug, question_pattern='write-?in')

distmap = {}
candseq = {}

for f in config.files:
    # Loop over input files
    with TSVReader(f.filename) as r:
        logging.debug(f"Reading file({f.filename})")

        is_master = f.file_type.endswith("-master")

        # Make sure all expected headers are present
        for k, h in f._asdict().items():
            if not k.endswith("_col"): continue
            if not h or h in r.header: continue
            raise RuntimeError(f"Expected {k} column '{h}' in {f.filename}: among {r.headerline}")

        for d in r.readdict():
            # Loop over input records
            if f.contest_type_col and f.skip_contest_types:
                if eval_config_pattern(d[f.contest_type_col],f.skip_contest_types):
                    # ignore lines with a contest type to skip
                    continue
            cont_id = d[f.contest_id_col]
            if not cont_id:
                continue    # Skip null contest IDs

            if f.district_id_col:
                distmap[cont_id] = d[f.district_id_col]

            if f.contest_name_col:
                # Handle contest definition
                # Skip duplicate contest IDs
                if (cont_id in m.cont_idname if is_master
                    else cont_id in m.cont_id2name):
                    continue
                name = d.get(f.contest_name_col, None)
                if f.question_pats:
                    # Check the contest name for a question pattern
                    (question, n) = eval_config_pattern_map(name, f.question_pats, True)
                    if not n and f.contest_text_col:
                        (question, n) = eval_config_pattern_map(
                            d[f.contest_text_col], f.question_pats, True)
                    if n:
                        name = question
                    else:
                        question = None
                else:
                    question = None

                m.enter_contest(is_master, cont_id, name, question)
            # End contest definition

            if f.candidate_name_col:
                # Handle candidate definition
                cand_id = cont_id+'.'+d[f.candidate_id_col] if f.candidate_id_col else None
                cand_name = d[f.candidate_name_col]
                if is_master:
                    m.enter_cand(cont_id, cand_id, cand_name)
                else:
                    m.lookup_cand(cont_id, cand_id, cand_name)
                if f.candidate_seq_col:
                    candseq[cand_id] =d[f.candidate_seq_col]
            # End candidate definition
        # End loop over input records
    # End processing f.filename
# End loop over input files

m.resolve_contests()

if not m.check_nocand_contests():
    print("Unmapped Source IDs:")
    for i in m.unmapped_cont_ids:
        print(f"{i} {m.cont_idname[i]}")
    print("No Candidate Source IDs:")
    for i in m.nocand_cont_ids:
        print(f"{i} {m.cont_idname[i]}")
    print("Unmapped Target IDs:")
    for i in m.unmapped_cont_id2s:
        print(f"{i} {m.cont_id2name[i]}")
    print("No Candidate Target IDs:")
    for i in m.nocand_cont_id2s:
        print(f"{i} {m.cont_id2name[i]}")

# contmap_header = "contest_id2|contest_id1|contest_name"
found_map = {}
with TSVWriter(f"contmap{args.suffix}.tsv", sep=separator,
               sort=False, header=contmap_header) as w:
    for id2,id1 in sorted(m.cont_map.items()):
        if id1 in found_map:
            print(f"ERROR:Duplicate {id1}:->{found_map[id1]}, {id2}\n")
        w.addline(id2, id1, m.cont_id2name[id2], m.cont_idname[id1])
        found_map[id1] = id2
    for id2 in m.unmapped_cont_id2s:
        w.addline(id2, "", m.cont_id2name[id2], "")

if len(distmap):
    with TSVWriter(f"distmap{args.suffix}.tsv", sep=separator,
                sort=False, header=distmap_header) as w:
        for id2,id1 in sorted(m.cont_map.items()):
            w.addline(id2, id1, distmap.get(id1,config.default_district_id))

m.resolve_candidates()

if m.unmapped_candinfo:
    print("Unmapped Source Candidates:")
    for i in m.unmapped_candinfo:
        print(f"{i['cand_id']} {i['cand_name']}")

if m.unmapped_candinfo2:
    print("Unmapped Target Candidates:")
    for i in m.unmapped_candinfo2:
        print(f"{i['cand_id']} {i['cand_name']}")

with TSVWriter(f"candmap{args.suffix}.tsv", sep=separator,
               sort=False, header=candmap_header) as w:
    for id2,id1 in sorted(m.cand_map.items()):
        cont_id1,cand_id1 = id1.split('.')
        cont_id2,cand_id2 = id2.split('.')
        w.addline(cont_id2,cand_id2, cont_id1,cand_id1,
                  candseq.get(id1,""),
                  m.cand_id2name.get(id2,""), m.cand_idname.get(id1,""))






