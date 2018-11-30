#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to fetch EMS download datasets for SF
"""

import os
import os.path
import re
import urllib.request
import argparse
from shutil import copyfileobj


DESCRIPTION = """\
Fetch election definition data from sfelections.org download datasets.
"""

VERSION='0.0.1'     # Program version

DEFAULT_DATE="2018/Nov"

DESCRIPTION = """\
Fetch DFM EMS download data from sfgov.org Data and Maps->Download Datasets.

Selected data is downloaded into emsdata-raw.
"""

MAPS_URL = "https://sfgov.org/elections/sites/default/files/Documents/Maps/2017lines.zip"

DATA_URL = "https://sfelections.org/tools/election_data/dataset.php"
# Complete URL has date suffix, e.g. 2018-11-06 or 1970-01-01
# https://sfelections.org/tools/election_data/dataset.php?ATAB=d2018-11-06
# Date suffix is used by javascript

# Date specific URL patterns:
# https://sfgov.org/elections/sites/default/files/Documents/ElectionsArchives/2018/Nov/PODJ011_PollingPlaces_20181001.txt
# https://sfgov.org/elections/ftp/uploadedfiles/elections/ElectionsArchives/opendata/2015nov/20151103_ConsolDistBT.txt

# Undated URL patterns:
# https://sfgov.org/elections/sites/default/files/Documents/ElectionsArchives/OpenData/PDMJ001_DistPctExtract.txt

#Listed in download, but related to results:
#https://www.sfelections.org/tools/election_data/data/2018-11-06/ED_VBM_PCT.csv
#https://www.sfelections.org/tools/election_data/output.php?data=1&E=2018-11-06


FILE_MAP = """\
PODJ011_PollingPlaces*.txt=PODJ011.tsv
CFMJ001_ContestCandidateData.txt=CFMJ001_ContestCandidateData.tsv
CFMContestData.txt=CFMJ001_ContestData.tsv
CFMJ008_MeasureData.txt=CFMJ008.tsv
PDMJ001_DistPctExtract.txt=PDMJ001.tsv
SGMJ001_StreetExtract*.txt=SGMJ001.tsv
ConsolDistBT_.*_NoDups.txt=ConsolDistBT.tsv
"""

def parse_args():
    """
    Parse sys.argv and return a Namespace object.
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION,
                    formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='enable verbose info printout')
    parser.add_argument('-t', dest='test', action='store_true',
                        help='test mode, print files to fetch')
    parser.add_argument('-r', dest='replace', action='store_true',
                        help='refetch and replace existing files')
    parser.add_argument('-E', dest='saveerrs', action='store_true',
                        help='save error response html with .err suffix')
    parser.add_argument('date', help='date select, e.g. 2018/Nov',
                        nargs='?', default=DEFAULT_DATE)

    args = parser.parse_args()

    return args

# Cloudflare blocks python!
# Send fake browser ID
urlheaders = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}

def getfile(url, filename):
    if args.replace or not os.path.isfile(filename):
        print(f'getfile: {filename} <- {url}')
        if args.test:
            return
        try:
            # Cloudflare blocks python's User-Agent: so we cannot use
            # urllib.request.Request
            #urllib.request.urlretrieve(url, filename)
            with urllib.request.urlopen(
                    urllib.request.Request(url, headers=urlheaders)) as infile:
                # Might need to check for Content-Encoding
                #reqinfo = infile.info()
                #print(reqinfo)
                with open(filename,'wb') as outfile:
                    copyfileobj(infile,outfile)
        except urllib.error.URLError as e:
            print(e)
            if args.saveerrs:
                with open(filename+".err",'w') as errfile:
                    errfile.write(e.read())

args = parse_args()

os.makedirs("emsdata-raw", exist_ok=True)

urlfile = open("emsdata-raw/urls.tsv",'w');

content = urllib.request.urlopen(
                    urllib.request.Request(DATA_URL, headers=urlheaders))

for lineb in content:
    line = lineb.decode('utf-8')
    m = re.search(r'href="([^"<>]*/Documents/ElectionsArchives/(OpenData|'+
                 args.date+r')/([^/"]+))"',  line)
    if not m:
        continue

    url, subdir, filename = m.groups()

    # Adjust output filename
    filename = re.sub(r'\.txt$','.tsv',filename)
    filename = re.sub(r'^CFMContestData','CFMJ001_ContestData', filename)
    m = (re.match(r'(CFMJ001\w+)',filename) or
         re.match(r'(ConsolDistBT).*_NoDups',filename) or
         re.match(r'([A-Z]{3}J\d\d\d)',filename))
    if m:
        filename = m.group(1)
        urlfile.write(f'{url}\t{filename}\n')
        getfile(url, f"emsdata-raw/{filename}.tsv")

    elif args.verbose:
        print(f"Skip {filename}")

getfile(MAPS_URL, "pctshapefiles.zip")



