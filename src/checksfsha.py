#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to check the sha512.csv against the sha512sum.txt
"""

import os
import os.path
import re
import argparse
import struct
from typing import List
from zipfile import ZipFile

DESCRIPTION = """\
Checks sha512.csv in election results data downloaded from
sfgov.org "Election Results - Detailed Reports" against
the computed sha512sum.txt

Reads the following files:
  * resultdata-raw.zip - Downloaded SF results
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

    args = parser.parse_args()

    return args

class FormatError(Exception):pass # Error matching expected input format

with ZipFile("resultdata-raw.zip") as rzip:
    rfiles = rzip.infolist()

    shav = {}
    shan = {}

    # Load sha512.csv values
    with rzip.open("sha512.csv") as f:
        line = f.readline()
        line = f.readline()
        for line in f:
            n, name, path, h = line.split(b',')[:4]
            name = re.sub(br'^20\d{6}_', b'', name)
            h = h.lower()

            shav[name] = h
            shan[h] = name
            #print(f"sha[{name}] = {h}")

    # Check sha512sum.txt values
    found = mismatch = 0;
    with rzip.open("sha512sum.txt") as f:
        for line in f:
            h, name = line.split()
            if h in shan:
                if name != shan[h]:
                    print(f"Mismatch name {name} becomes {shan[h]}")
                found += 1
            elif name in shav and shav[name] != h:
                print(f"Mismatch for {name}\n{h} !=\n{sha[name]}\n")
                mismatch += 1
            elif name != b'sha512.csv' and name != b'urls.tsv':
                print(f"No match for {name}")


    print(f"{found} Found, {mismatch} Mismatched")