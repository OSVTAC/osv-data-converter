#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to convert sample ballot PDFs to html and extract contest/candidates
"""

import os
import os.path
import re
import urllib.request
import argparse
from shutil import copyfileobj

VERSION='0.0.1'     # Program version

DESCRIPTION = """\
Convert PDF to html and analyze text
"""

PDFTOHTML = "pdftohtml -s -i"

def parse_args():
    """
    Parse sys.argv and return a Namespace object.
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION,
                    formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='enable verbose info printout')

    args = parser.parse_args()

    return args


args = parse_args()

m = re.search(r'/(20\d\d)-(\d\d)-\d\d/',os.getcwd())
date = f"{m[1]}_{m[2]}"

for i in range(args.endbt):
    bt = str(i+1).zfill(2)
    url = URL_FORMAT.format(date,bt)
    getfile(url, f"BT_{bt}_S.pdf")
