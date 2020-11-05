#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to fetch turnout download files for SF

"""

import os
import os.path
import sys
import re
import urllib.request
import argparse
from shutil import copyfileobj
from lxml import html

DESCRIPTION = """\
Fetch election turnout data from sfgov.org Election Results - Turnout.

The first found (most recent) results URLs are retrieved from the index
page and saved in the subdirectory "turnoutdata-raw".
"""

VERSION='0.0.1'     # Program version



VBM_SUMMARY_URL = "https://sfelections.org/tools/election_data/output.php?data=1&E=2018-11-06"
VBM_PRECINCT_URL = "https://sfelections.org/tools/election_data/data/2018-11-06/ED_VBM_PCT.csv"
VBM_CHALLENGES_URL = "https://sfelections.org/tools/election_data/vbm_challenge_0.php?E=2018-11-06"
VC_TURNOUT_URL = "https://sfelections.org/tools/election_data/vc.php?E=2018-11-06"
REG_STAT_URL = "https://www.sfelections.org/tools/election_data/"

# To substitute the desired date in the URL pattern
url_date_re = re.compile('2018-11-06')


def parse_args():
    """
    Parse sys.argv and return a Namespace object.
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION,
                    formatter_class=argparse.RawDescriptionHelpFormatter)

    # Get default election date from cwd
    m = re.search(r'/(20\d\d-\d\d-\d\d)/', os.getcwd())
    default_date = m.group(1) if m else None

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
    parser.add_argument('date', help='yyyy-mm-dd election date', nargs='?',
                        default=default_date)

    args = parser.parse_args()

    if not re.match('20\d\d-\d\d-\d\d$',args.date):
        parser.print_help(sys.stderr)
        sys.exit(1)

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
        except urllib.error.URLError as e:
            print(e)
            if args.saveerrs:
                with open(filename+".err",'w') as errfile:
                    errfile.write(e.read())

# Get the urls in filemap
filemap = {
    "vbmsummary.csv": VBM_SUMMARY_URL,      # VBM ballots by party for each day
    "vbmprecinct.csv": VBM_PRECINCT_URL,    # VBM ballots by party for each precinct
    "vbmchallenges.html": VBM_CHALLENGES_URL,   #
    "vcturnout.html": VC_TURNOUT_URL,
    "regstat.html":REG_STAT_URL,
    }

def extract_table(infile, separator):
    """
    Load an html file and convert a single table to tsv
    """
    outfile = re.sub('\.html$','.tsv',infile)
    with open(infile) as f:
        html = f.read()
        html = re.sub(r'^.*<table[^<>]*>\s*','',html, flags=re.S)
        html = re.sub(r'\s*</table[^<>]*>.*$','',html, flags=re.S)
        html = re.sub(r'\s*<a[^<>]*>.*?</a>\s*','',html, flags=re.S)
        html = re.sub(r'\s*(<[^<>]*>)\s*',r'\1',html, flags=re.S)
        html = re.sub(r'<(tr|td|th|/?thead|/?tbody)[^<>]*>','',html)
        html = re.sub(r'</(td|th)[^<>]*>',separator,html)
        html = re.sub(r'[\|\t]*</tr[^<>]*>','\n',html)
        with open(outfile,'w') as of:
            of.write(html)

def main():

    global args
    args = parse_args()

    separator = "|" if args.pipe else "\t"

    os.makedirs("turnoutdata-raw", exist_ok=True)

    urlfile = open("turnoutdata-raw/urls.tsv",'w');

    for filename, url in filemap.items():
        url = url_date_re.sub(args.date, url)
        getfile(url,"turnoutdata-raw/"+filename)
        urlfile.write(f'{url}\t{filename}\n')

    # Convert html to tsv
    extract_table("turnoutdata-raw/vbmchallenges.html", separator)
    extract_table("turnoutdata-raw/vcturnout.html", separator)

    # parse complex registration stats
    doc = html.parse("turnoutdata-raw/regstat.html")
    tabs = doc.xpath('//div[@class="tab-pane"]|//div[@class="tab-pane active"]')
    line = 0
    with open("turnoutdata-raw/regstat.tsv",'w') as f:
        for tab in tabs:
            Id = tab.get('id')
            rows = tab.cssselect('table tr')
            #print(f"id={Id} rows={len(rows)}")
            data = [
                    [td.text_content().strip() for td in row.cssselect('td') ]
                    for row in rows]
            if not line:
                print("Area"+separator+separator.join([td[0] for td in data if td]),file=f)
            print(Id+separator+separator.join([td[1] for td in data if td]),file=f)
            line += 1


if __name__ == '__main__':
    main()
