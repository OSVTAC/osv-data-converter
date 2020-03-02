#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to compare registration and turnout (ballots cast) between 2 contest
result files

"""

import os
import os.path
import sys
import re
import argparse

from tsvio import TSVReader

DESCRIPTION = """\
Compare a pair of contest registration and turnout and print areas different
"""

VERSION='0.0.1'     # Program version

def parse_args():
    """
    Parse sys.argv and return a Namespace object.
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION,
                    formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='enable verbose info printout')
    parser.add_argument('contests', nargs=2,
                        help='contest IDs to check')

    args = parser.parse_args()

    return args


args = parse_args()



fn1,fn2 = [f"../out-orr/resultdata/results-{Id}.tsv" for Id in args.contests]

with TSVReader(fn1) as f1, TSVReader(fn2) as f2:

    for (l1,l2) in zip(f1.readlines(),f2.readlines()):
        if not (l1[0].startswith('PCT') or l1[0].startswith('ALL')): continue
        if l1[2]!=l2[2] or l1[3]!=l2[3]:
            print(l1[0:4],l2[2:4])



