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

DESCRIPTION = """\
Creates a candlist-fix.tsv with writein candidates in ../resultdata/.

Reads the following files:
  * candlist-omni.tsv - current candidates
  * contmap.tsv - map resultdata ID to omni ID
  * ../resultdata/candlist-sov.tsv - candlist with writeins

Creates the following files:
  * candlist-fix.tsv
"""
VERSION='0.0.1'     # Program version

DFM_ENCODING = 'ISO-8859-1'
OUT_ENCODING = 'UTF-8'

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
    parser.add_argument('-p', dest='pipe', action='store_true',
                        help='use pipe separator else tab')

    args = parser.parse_args()

    return args

args = parse_args()

if args.debug:
    logging.basicConfig(level=logging.DEBUG)

candlist_header = "sequence|cont_external_id|cand_seq|cand_id|external_id"\
    "|title|party|designation|cand_type"

candlist_sov_header = "contest_id|candidate_order|contest_id_eds|candidate_id|candidate_type"\
    "|candidate_full_name|candidate_party_id|is_writein_candidate"

separator = "|" if args.pipe else "\t"

cont_id2seq = {}
cont_id2candseq = {}

with TSVReader("candlist-omni.tsv") as r:
    if r.headerline != candlist_header:
        raiseFormatError(f"Unmatched candlist-omni.tsv header {r.headerline}")
    for cols in r.readlines():
        (sequence, cont_external_id, cand_seq, cand_id, external_id, title,
            party, designation, cand_type) = cols

        cont_id2seq[cont_external_id] = sequence
        cont_id2candseq[cont_external_id] = int(cand_seq)

with TSVReader("contmap.tsv") as r:
    contmap = r.load_simple_dict(1,0)

cand_id = 99000
external_id = 9900

with TSVWriter("candlist-fix.tsv", sep=separator,
               sort=False, header=candlist_header) as w:

    with TSVReader("../resultdata/candlist-sov.tsv") as r:
        if r.headerline != candlist_sov_header:
            raiseFormatError(f"Unmatched candlist-sov.tsv header {r.headerline}")
        for cols in r.readlines():
            (contest_id, candidate_order, contest_id_eds, candidate_id,
            candidate_type, candidate_full_name, candidate_party_id,
            is_writein_candidate) = cols

            if is_writein_candidate != "1" or candidate_full_name == "WRITE-IN":
                continue

            cont_external_id = contmap[contest_id]
            sequence = cont_id2seq[cont_external_id]
            cont_id2candseq[cont_external_id] += 1
            cand_seq = str(cont_id2candseq[cont_external_id]).zfill(3)

            cand_id += 1
            external_id += 1

            w.addline(sequence, cont_external_id,
                            cand_seq, cand_id, external_id,
                            candidate_full_name, "", "Write-In", "writein")






