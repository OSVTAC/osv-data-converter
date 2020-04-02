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

def config_prefix_map(l:List[str])->List[Tuple[str,str]]:
    """
    Splits lines of the form str=str into a list of (str,str) tuples
    """
    if l is None:
        return []
    retval = []
    for line in l:
        m = re.match(r'^(.*?)=(.*)', line)
        if not m:
            raise InvalidConfig(f"Invalid Config Pattern Map '{line}'")
        retval.append((m.group(1),m.group(2)))
    return retval

def eval_prefix_map(v:str,                      # Code value to check
                    patlist:List[Tuple[str,str]] # List of (prefix,retval)
                    )->str:                     # Returns value found or None
    for (pat,retval) in patlist:
        if v.startswith(pat):
            return retval
    return None


CONFIG_FILE = "config-fixnames.yaml"
config_attrs = {
    "district_code_classification": config_prefix_map,
    "default_district_id": [str]
    }

config = Config(CONFIG_FILE, valid_attrs=config_attrs)

separator = "|" if args.pipe else "\t"

with TSVReader("distnames.tsv", validate_header=distnames_header) as r:

    with TSVWriter("distclass.tsv", False, separator, distclass_header) as w:

            for (District_Code, District_Name, District_Short_Name) in r.readlines():

                c = eval_prefix_map(District_Code.upper(),
                                    config.district_code_classification)

                if c==None:
                    c = ''
                w.addline(District_Code, c, District_Name, District_Short_Name)
            # End loop over input lines
        # End writing output file
    # End reading input file
