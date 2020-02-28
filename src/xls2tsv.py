#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to convert an xls/xlsx formatted spreadsheet to TSV. Worksheets are
separated by a leading \f.

This uses xlrd not openpyxl because openpyxl is too slow (>20 min to load
a worksheet).

"""

import logging
import os
import os.path
import re
import argparse
import xlrd
import datetime
from math import floor
from typing import Dict, Tuple, List, TextIO, Union, Pattern
from tsvio import TSVReader, TSVWriter, DuplicateError, map_psv_data, map_tsv_data

DESCRIPTION = """\
Converts one or more XLS/XLSX spreadsheet files into tab-separated-values (tsv)
or pipe (|) separated values (psv) with the -p option.

Files converted will have the same name but tsv/psv suffix.

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
    parser.add_argument('-D', '--debug', action='store_true',
                        help='enable debug logging')
    parser.add_argument('-p', dest='pipe', action='store_true',
                        help='use pipe separator else tab')
    parser.add_argument('-o', '--outdir',
                        help='place output files/paths under OUTDIR')
    parser.add_argument('infile', nargs='+',
                        help='XLS/XLSX input file(s)')

    args = parser.parse_args()

    return args

args = parse_args()

if args.debug:
    logging.basicConfig(level=logging.DEBUG)

if args.pipe:
    separator = "|"
    suffix = ".psv"
    map_data = map_psv_data
else:
    separator = "\t"
    suffix = ".tsv"
    map_data = map_tsv_data

def format_value(cell):
    """
    Returns the string value representing an xlrd cell.
    """
    if cell.value==None:
        return ''
    if cell.ctype==xlrd.XL_CELL_DATE:
        dt = datetime.datetime(*xlrd.xldate_as_tuple(cell.value, wb.datemode))
        # We could check seconds to format as a date
        if dt.time()==datetime.time.min:
            return dt.date().isoformat()
        else:
            return dt.isoformat(sep=' ')
    if cell.ctype==xlrd.XL_CELL_NUMBER:
        if cell.value==floor(cell.value):
            return(str(int(cell.value)))
    return str(cell.value)



for f in args.infile:
    outf = re.sub(r'\.xlsx?$', '', f, flags=re.I) + suffix
    if args.outdir:
        outf = re.sub(r'^(.*/)?(.*)', args.outdir+r'\2', outf)

    if args.verbose:
        print(f"reading {f} creating {outf}\n")

    try:
        wb = xlrd.open_workbook(f, on_demand=True)
        ws_separator = '' # \f between worksheets
        with TSVWriter(outf, sep=separator, sort=False, map_data=map_data) as w:
            for wsname in wb.sheet_names():
                logging.debug(f'Reading Worksheet {wsname}')
                ws = wb.sheet_by_name(wsname)

                if ws_separator:
                    w.f.write(ws_separator)

                for row in ws.get_rows():
                    w.addline(*(format_value(v) for v in row))
                ws_separator = '\f'


    except Exception as ex:
        print(f"Error converting {f}")
        print(ex)





