#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to fetch voter registration statistic download files from the
California Sec

"""

import os
import os.path
import sys
import re
import urllib.request
import argparse
from shutil import copyfileobj

DESCRIPTION = """\
Fetch voter registration data from sos.ca.gov.

The first found (most recent) results URLs are retrieved from the index
page and saved in the subdirectory "turnoutdata-raw".
"""

VERSION='0.0.1'     # Program version

REG_INDEX_URL = 'https://www.sos.ca.gov/elections/report-registration/'


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
                content = infile.read()
                if content.startswith(b'\xff\xfeD'):
                    print(f"Found UTF-16 in {filename}")
                    content = content.decode('UTF-16').encode('UTF-8')
                with open(filename,'wb') as outfile:
                    outfile.write(content)

                if filename.endswith(".xlsx"):
                    os.system(f"xls2tsv.py {xls2tsv_opts} '{filename}'")


        except urllib.error.URLError as e:
            print(e)
            if args.saveerrs:
                with open(filename+".err",'w') as errfile:
                    errfile.write(e.read())

args = parse_args()

xls2tsv_opts = " -p" if args.pipe else ""

subdir = ''

content = urllib.request.urlopen(REG_INDEX_URL)
for lineb in content:
    line = lineb.decode('utf-8')
    # Content is on one long line
    m =re.search(r'.*href="/elections/report-registration/([^"<>]+)/"',  line)
    if m:
        subdir = m.group(1)
        # Discrepancy in file naming
        subdir = re.sub(r'-(\d\d)$',r'-20\1', subdir)
        if args.verbose:
            print(f"Subdir:{subdir}")

if not subdir:
    print(f"Unmatched report directory in {REG_INDEX_URL}")
    exit(1)
# https://elections.cdn.sos.ca.gov/ror/60day-presprim-2020/county.xlsx
# https://elections.cdn.sos.ca.gov/ror/60day-presprim-2020/politicalsub.xlsx
for filename in ['county.xlsx','politicalsub.xlsx']:
    getfile(f"https://elections.cdn.sos.ca.gov/ror/{subdir}/{filename}", filename)
