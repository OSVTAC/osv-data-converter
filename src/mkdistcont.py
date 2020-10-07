#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Create the distcont.tsv list of districts in the election with contests
by voting district.

"""

import argparse
import os.path
import re
from zipfile import ZipFile
from tsvio import TSVReader, TSVWriter
from collections import defaultdict
from typing import Union, List, Pattern, Match, Dict, Tuple, Optional

import configEMS
from distpctutils import load_distnames

DESCRIPTION = """\
Collects the list of contests by district in the contlist and measlist
to create the list of districts in the election.

Reads the following files:
  * contlist-orig.tsv
  * measlist-orig.tsv
  * distclass.tsv
 
Creates the following files:
  * distcont.tsv
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
    parser.add_argument('-t', '--traceinp', action='store_true',
                        help='print input found')

    args = parser.parse_args()

    return args

args = parse_args()
config = configEMS.load_ems_config()

separator = config.tsv_separator

distcont = defaultdict(lambda:list())

def find_file(names:str)->Optional[str]:
    """
    Returns the first file found to exist in the space separated
    list of names, or None.
    """
    for n in names.split():
        if os.path.exists(n):
            return n
    return None

def scan_file(
        filename:str,       # File to open
        header: str         # header line
        ):
    """
    Reads the tsv file with contest_id, district_id, ballot_title,
    and optionally on_ballot.
    """
    if not filename:
        return
    with TSVReader(filename, validate_header=header) as r:
        for c in r.readtuple(filename[:-4]):
            if getattr(c, 'on_ballot','')=='N':
                continue
            district_id = config.map_contest_district(
                            c.ballot_title, c.district_id)
            distcont[district_id].append(c.contest_id)

contlist_header = "contest_seq|contest_id|district_id|headings|ballot_title|contest_abbr"\
    "|contest_party|vote_for|on_ballot"

measlist_header = "contest_seq|contest_id|district_id|headings|ballot_title|contest_abbr"\
    "|choice_names"

distcont_header = "district_id|district_name|contest_ids"

scan_file(find_file("contlist.tsv contlist-orig.tsv"),contlist_header)
scan_file(find_file("measlist.tsv measlist-orig.tsv"),measlist_header)

# Load the district name dictionary
distnames = load_distnames()

with TSVWriter("edistcont.tsv", sep=separator, header=distcont_header) as w:
    for district_id, contest_ids in distcont.items():
        w.addline(district_id, distnames[district_id], ' '.join(sorted(contest_ids)))

