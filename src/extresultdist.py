#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to extract reporting_group_ids from the converted detailed results


"""

import logging
import os
import os.path
import re
import glob
import argparse
from typing import Dict, Tuple, List, TextIO, Union, Pattern
from tsvio import TSVReader, TSVWriter, DuplicateError

DESCRIPTION = """\
Scans the ../out-orr/resultdata/results-*.tsv to collect.

Reads the following files:
  * ../out-orr/resultdata/results-*.tsv
  * contmap.tsv (SOV ID to omniballot ID
  * distmap.ems.tsv (district cocde to omniballot ID)

Creates the following files:
  * reporting_groups.tsv
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

reporting_groups_header = "district_id|reporting_group_ids"

separator = "|" if args.pipe else "\t"

# Load the contest maps
with TSVReader("contmap.tsv") as r:
    # Convert sov contest to omniballot contest
    contmap = r.load_simple_dict(1,0)

with TSVReader("distmap.ems.tsv") as r:
    # Convert sov contest to omniballot contest
    distmap = r.load_simple_dict(0,2)

cont_by_dist = {}

with TSVWriter("reporting_groups.tsv", sep=separator, sort=False,
               header=reporting_groups_header, unique_col_check=0) as w:
    for f in glob.iglob("../out-orr/resultdata/results-*.tsv"):
        # Loop over input files
        try:
            sov_id = re.search('/results-(.+)\.tsv$',f).group(1);
            district_id = distmap[contmap[sov_id]]
        except Exception as ex:
            print(f"ERROR: Cannot find district for result file {f} sov={sov_id}\n");
            exit(0)
            continue

        with TSVReader(f) as r:
            logging.debug(f"Reading file({f})")

            # Collect the area~subtotal IDs
            items = [];

            for cols in r.readlines():
                if cols[0].startswith("RCV"):
                    continue
                items.append(f"{cols[0]}~{cols[1]}")

            reporting_group_ids = ' '.join(items)
            try:
                w.addline(district_id,reporting_group_ids)
            except:
                print(f"Mismatch for dist {district_id} cont {sov_id} prior {cont_by_dist[district_id]}\n")

            cont_by_dist[district_id] = sov_id



