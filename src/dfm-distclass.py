#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to derive district classification for each district


"""

import re
import argparse
from tsvio import TSVReader, TSVWriter, DuplicateError
from config import Config, config_whole_pattern_list, eval_config_pattern, eval_config_pattern_map, config_whole_pattern_map, PatternMap
from typing import Union, List, Pattern, Match, Dict, Tuple

import configEMS

from configems import eval_prefix_map, load_ems_config

DESCRIPTION = """\
Reads the distnames.tsv, computes a classification, then creates the
distclass.tsv according to the config-fixnames.yaml.

"""
VERSION='0.0.1'     # Program version

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

    args = parser.parse_args()

    return args

args = parse_args()

distnames_header = "District_Code|District_Name|District_Short_Name"
distclass_header = "District_Code|Classification|District_Name|District_Short_Name"

config = configEMS.load_ems_config()

separator = "|" if args.pipe else "\t"

with TSVReader("distnames.tsv", validate_header=distnames_header) as r:

    with TSVWriter("distclass.tsv", False, separator, distclass_header) as w:

            for (District_Code, District_Name, District_Short_Name) in r.readlines():

                c = configEMS.eval_prefix_map(District_Code.upper(),
                                    config.district_code_classification)

                if c==None:
                    c = ''
                w.addline(District_Code, c, District_Name, District_Short_Name)
            # End loop over input lines
        # End writing output file
    # End reading input file
