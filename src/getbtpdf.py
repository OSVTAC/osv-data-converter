#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to fetch sample ballot PDFs for all ballot types
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

DESCRIPTION = """\
Fetch a set of sample ballot PDFs for each ballot type based on a pattern.
"""

URL_FORMAT = "https://www.sfelections.org/sample_ballots/{}/BT_{}_S.pdf"

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
    parser.add_argument('endbt', type=int, help='number of ballot types')
#    parser.add_argument('url', help='URL to fetch')
#    parser.add_argument('btpat', help='ballot type pattern in url')

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

m = re.search(r'/(20\d\d)-(\d\d)-\d\d/',os.getcwd())
date = f"{m[1]}_{m[2]}"

for i in range(args.endbt):
    bt = str(i+1).zfill(2)
    url = URL_FORMAT.format(date,bt)
    getfile(url, f"BT_{bt}_S.pdf")
