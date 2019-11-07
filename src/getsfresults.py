#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to fetch results download datasets for SF

TODO:
    * store data in resultsdata/resultdata-raw.zip
    * compute/verify sha512sum.txt
    * add digital signature in sha512sum.txt.sig
"""

import os
import os.path
import re
import urllib.request
import argparse
from shutil import copyfileobj

DESCRIPTION = """\
Fetch election results data from sfgov.org Election Results - Detailed Reports.

The first found (most recent) results URLs are retrieved from the index
page and saved in the subdirectory "resultdata-raw".
"""

VERSION='0.0.1'     # Program version

DEFAULT_URL = "https://sfelections.sfgov.org/november-5-2019-election-results-detailed-reports"

# VBM Turnout is on the Data Downloads page
# https://www.sfelections.org/tools/election_data/data/2018-11-06/ED_VBM_PCT.csv
# TODO: Add the ED_VBM
# VBM history (totals by day
# https://www.sfelections.org/tools/election_data/output.php?data=1&E=2018-11-06
# Stored in vbm_turnout_day.tsv


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
    parser.add_argument('-p', dest='pipe', action='store_true',
                        help='use pipe separator else tab')
    parser.add_argument('-X', dest='noxml', action='store_true',
                        help='exclude xml format')
    parser.add_argument('url', help='URL of the detailed reports index page',
                        nargs='?', default=DEFAULT_URL)

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

                if filename.endswith(".xlsx"):
                    os.system(f"xls2tsv.py {xls2tsv_opts} '{filename}'")

        except urllib.error.URLError as e:
            print(e)
            if args.saveerrs:
                with open(filename+".err",'w') as errfile:
                    errfile.write(e.read())

args = parse_args()

xls2tsv_opts = " -p" if args.pipe else ""

os.makedirs("resultdata-raw", exist_ok=True)

urlfile = open("resultdata-raw/urls.tsv",'w');

foundRelease = None # First release # found is the one to download

rcv_prefixes = set()

def processfile(url, subdir, filename):
    if args.verbose:
        print(f"url:{url}")

    if filename.endswith(".xls"):
        # Bug
        filename += "x"
        url += "x"

    if args.noxml and filename.endswith("sov.xml"):
        # Skip the verbose xml
        return;

    urlfile.write(f'{url}\t{filename}\n')

    if re.match(r'^CVR_Export_\w+\.zip',filename):
        getfile(url,"CVR_Export.zip")
    else:
        getfile(url,"resultdata-raw/"+filename)
    # break; # for debug
    if subdir is not None:
        rcv_prefixes.add(subdir)

content = urllib.request.urlopen(args.url)
lastRelease = ''
for lineb in content:
    line = lineb.decode('utf-8')
    m =re.search(r'href="([^"<>]*/20\d{6}/data/(20\d{6}(?:_\d)?)/(?:([^/"]+)/)?(?:20\d{6}_(?:\d_)?)?([^/"]+))"',  line)
    if not m:
        continue

    url, release, subdir, filename = m.groups()
    if not (foundRelease or re.search(r'sov.xls',filename)):
        if release != lastRelease:
            priorfiles = []
            lastRelease = release
        priorfiles.append((url,subdir,filename))
        continue
    if foundRelease is None:
        foundRelease = release
        for t in priorfiles:
            processfile(*t)
    elif release != foundRelease:
        break;
    processfile(url, subdir, filename)


prefixpat = "|".join(rcv_prefixes)
print(f'RCV_prefixes={prefixpat}')

prefout = open('resultdata-raw/rcv_prefixes.txt','w');
prefout.write(prefixpat);



