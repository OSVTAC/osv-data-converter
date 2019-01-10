#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to normalize the ballot type to contest map in
emsdata-raw/EWMJ014_ContestBalTypeXref.txt

"""

import argparse
from zipfile import ZipFile
from tsvio import TSVReader, TSVWriter

DESCRIPTION = """\
Converts DFM EWMJ014_ContestBalTypeXref.txt list of contest IDs and
rotation ID by ballot type

Reads the following files:
  * resultdata-raw.zip/EWMJ014_ContestBalTypeXref.txt

Creates the following files:
  * btcont.tsv - ballot type with list of contest_id:rotation IDs
  * contlist-ewm.tsv - contest ID and abbreviated names found
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
    parser.add_argument('-p', dest='pipe', action='store_true',
                        help='use pipe separator else tab')

    args = parser.parse_args()

    return args

args = parse_args()

headerline = "ELECTIONABBR|CONTESTID|CONTESTABBR1|CONTESTABBR2|BALLOTTYPE"\
    "|ROTATION"

separator = "|" if args.pipe else "\t"

with TSVWriter("contlist-ewm.tsv",
                header="contest_id|contest_abbr",
                sep=separator,
                unique_col_check=0) as contfile:

    with ZipFile("ems-raw.zip") as rzip:
        with TSVReader("EWMJ014_ContestBalTypeXref.txt", opener=rzip,
                    binary_decode=True, encoding=DFM_ENCODING) as r:
            if r.headerline != headerline:
                print(f"Mismatched header:\n   {r.headerline}\n!= {headerline}")
                exit(1)
            bt = {} # Contest list per bt found
            for (ELECTIONABBR, CONTESTID, CONTESTABBR1, CONTESTABBR2, BALLOTTYPE,
                    ROTATION) in r.readlines():
                # Save the list of contests with abbreviated name
                if CONTESTABBR2:
                    CONTESTABBR1 += '~' + CONTESTABBR2

                contfile.addline(CONTESTID, CONTESTABBR1)

                # Append the contest id:rotation code to list by ballot type
                if BALLOTTYPE not in bt:
                    bt[BALLOTTYPE] = []
                if ROTATION and ROTATION != "0":
                    CONTESTID += ':'+ROTATION
                bt[BALLOTTYPE].append(CONTESTID)



        with TSVWriter("btcont.tsv",
                        header="ballot_type|contest_rot_ids",
                        sep=separator) as w:
            for bt, contlist in bt.items():
                w.addline(bt.zfill(3), ' '.join(contlist))


