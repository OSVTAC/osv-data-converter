#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Read the ems-raw.zip/PDMJ001.tsv

"""

import argparse
import os.path
import re
from zipfile import ZipFile
from tsvio import TSVReader, TSVWriter
from collections import defaultdict
from typing import Union, List, Pattern, Match, Dict, Tuple, Optional

import configEMS
import distpctutils

from distpctutils import (write_distpct, write_pctdist, distpct_inverse)

DESCRIPTION = """\
Converts DFM list of district names and precincts.


Reads the following files:
  * emsdata-raw.zip/PDMJ001.tsv
 
Creates the following files:
  * distname-orig.tsv
  * pctname-orig.tsv
  * distpct.tsv.gz
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

pdm_headers = {
"sDistrictID|iSubDistrict|szDistrictName|sPrecinctID|sPrecinctPortion|szPrecinctName",
"sDistrictID|iSubDistrict|szDistrictName|sPrecinctID|sPrecinctPortion|szPrecinctName|dtLastUpdateDate",
}
# EWM data:
# "sElectionAbbr|sPrecinctID|sPrecinctPortion|sElecDistrictID|iElecSubDistrict",
# "sElectionAbbr|sElecDistrictID|iElecSubDistrict|szElecDistrictName",

distpct = defaultdict(lambda: set())

with TSVWriter('distname-orig.tsv', sep=separator, unique_col_check=0,
                header="district_id|district_name") as distname:
  with TSVWriter('pctname-orig.tsv', sep=separator, unique_col_check=0,
                header="precinct_id|precinct_name") as pctname:
    with ZipFile("ems-raw.zip") as rzip:
        with TSVReader("PDMJ001.tsv", opener=rzip, strip_spaces=True,
                    binary_decode=True, encoding=DFM_ENCODING,
                    validate_header=pdm_headers) as r:

            for l in r.readtuple('PDMJ001'):
                # District code 0 has a * prefix
                district_id = l.sDistrictID.strip('*')
                # If there is a subdistrict, add 2 digit - suffix
                if l.iSubDistrict and l.iSubDistrict != '0':
                    district_id += '-' + l.iSubDistrict.zfill(2)

                distname.addline(district_id, l.szDistrictName)

                precinct_id = l.sPrecinctID + l.sPrecinctPortion

                pctname.addline(precinct_id, l.szPrecinctName)

                # TODO expand splits
                distpct[district_id].add(precinct_id)

    for district_id, district_name in distpctutils.enter_distextra(distpct).items():
        distname.addline(district_id, district_name)

write_distpct(distpct, sep=separator)
write_pctdist(distpct_inverse(distpct), sep=separator)
    




