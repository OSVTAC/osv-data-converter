#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to create fake measure list output file:

"""

import re
import argparse
from zipfile import ZipFile
from tsvio import TSVReader, TSVWriter, DuplicateError

DESCRIPTION = """\
Creates fake EMS measure data for a list of measures

Creates the following files:
  * measlist-orig.tsv
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
    parser.add_argument('-p', dest='pipe', action='store_true',
                        help='use pipe separator else tab')
    parser.add_argument('-d','--district', default='0',
                        help='district code')
    parser.add_argument('measures', nargs='+',
                        help='measure letters to generate')

    args = parser.parse_args()

    return args

args = parse_args()



meas_header = "contest_seq|contest_id|district_id|headings|ballot_title|contest_abbr|choice_names"

separator = "|" if args.pipe else "\t"

contest_id = 9000
district_id = args.district

# Compute a sequence number of original input lines
seq = 0;
with TSVWriter("measlist-orig.tsv",
                sort=False,
                header=meas_header,
                sep=separator) as w:

    for measure in args.measures:
        seq += 1

        w.addline(str(seq).zfill(3), str(contest_id+seq), district_id,
                    "",measure,f"Measure {measure}","Yes~No")
    # End loop over input lines


