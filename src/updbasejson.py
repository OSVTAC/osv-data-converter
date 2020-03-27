#!/usr/bin/env python3
# Copyright (C) 2020  Carl Hage
#
# This file is part of Open Source Voting Data Converter (ODC).
#
# ODC is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Program to update the translations in a json file, and also perform selected
edits on the schema.
"""

import argparse
import json
from pathlib import Path
import sys
import os
import os.path
import re
import logging

from translations import Translator

DESCRIPTION = """\
Upgrade translations in a json file and perform auto-edits.

Renames the selected *.json files to *.json.bak (unless already existing),
then edits the .json.bak file to create a new .json.

"""

VERSION='0.0.1'     # Program version


NOPP_JSON_DUMP_ARGS = dict(sort_keys=False, separators=(',\n',':'), ensure_ascii=False)
DEFAULT_JSON_DUMP_ARGS = dict(sort_keys=False, indent=4, ensure_ascii=False)


TRANSLATIONS_FILE = (os.path.dirname(__file__)+
                     "/../submodules/osv-translations/translations.json")
def parse_args():
    """
    Parse sys.argv and return a Namespace object.
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION,
                    formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='enable verbose info printout')
    parser.add_argument('-H', '--noheadings', action='store_true',
                        help='remove heading if redundant with name')
    parser.add_argument('-D', '--debug', action='store_true',
                        help='enable debug logging')
    parser.add_argument('-k', dest='keep', action='store_true',
                        help="don't update prior translations")
    parser.add_argument('-r', dest='replace', action='store_true',
                        help="remove prior translations")
    parser.add_argument('-T', dest='notrans', action='store_true',
                        help="remove 'translations' section")
    parser.add_argument('-u', dest='ugly', action='store_true',
                        help="don't pretty-print json output")
    parser.add_argument('-w', dest='writein', action='store_true',
                        help="Convert Writein to Write-in")
    parser.add_argument('infile', nargs='+',
                        help='json file(s) to update')

    args = parser.parse_args()

    return args

args = parse_args()

json_dump_args = NOPP_JSON_DUMP_ARGS if args.ugly else DEFAULT_JSON_DUMP_ARGS

def convjson(j):
    """
    Recursively process json dicts to locate attributes with a translation
    and lookup new/replacement translations. A dict attribute with an 'en'
    key is assumed to be a translation.
    """
    if isinstance(j, list):
        for li in j: convjson(li)
        # Doesn't work: map(convjson, j)
        return
    elif not isinstance(j, dict):
        return

    # This is a dict element to check
    foundTrans = ''
    # Check each dict member for itext
    for k, v in j.items():
        if not (isinstance(v, dict) and 'en' in v):
            # Not itext, process recursively
            convjson(v)
            continue;

        en = v['en']
        foundTrans = en
        t = translator.lookup_phrase(en)
        if not t:
            not_found.add(en)
            continue

        attrnames_found.add(k)
        if args.replace:
            # Replace all entries with the new translations
            j[k] = t
            continue

        for l, s in t.items():
            if l in v:
                # A translation exists, check to keep existing
                if args.keep:
                    continue
            v[l] = s

    # Check to remove 'heading'
    if args.noheadings and 'heading' in j:
        if foundTrans:
            # Warn if different
            if j['heading'] != foundTrans :
                print(f"Warning heading {j['heading']} ne {foundTrans}")
            else:
                j.pop('heading')
        else:
            h = j.pop('heading')
            if args.writein:
                h = re.sub(r'ritein\b','rite-in',h)
            j['name'] = translator.get(h)


# Get the translator with loaded translation database
translator = Translator(TRANSLATIONS_FILE)

# Keep a list of the attributes translated
attrnames_found = set()
not_found = set()

for f in args.infile:
    if f.endswith('.bak'):
        f = f[:-4]
    if not f.endswith('.json'):
        print(f"ERROR: File {f} must end in .json")
        continue

    bakf = f+'.bak'
    if not os.path.exists(bakf):
        if not os.path.exists(f):
            print(f"ERROR: File {f} does not exist")
            continue
        os.rename(f, bakf)

    with open(bakf) as i:
        basejson = json.load(i)

        if args.notrans and 'translations' in basejson:
            basejson.pop('translations')

        convjson(basejson)

        with open(f,'w', encoding='utf-8') as out:
            json.dump(basejson, out, **json_dump_args)
            out.write("\n")


if attrnames_found:
    print(f"Attributes Found: {attrnames_found}")
if attrnames_found:
    print(f"Translations not Found: {not_found}")
