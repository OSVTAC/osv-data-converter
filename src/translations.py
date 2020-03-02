#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
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
#
"""
Routines to lookup translations in converted data
"""

import re
import utils

from typing import List, Pattern, Match, Dict, NewType, Tuple, Any, NamedTuple, Set

# HACK:Map _param names to regex here hard-coded
PARAM_NAME_REGEX = [
    (re.compile(r'(_count|_number)$'),r'\d+')
    ]

def form_translate_key(s:str)->str:
    """
    Convert a case sensitive phrase name into a dictionary lookup
    key, independent of case and punctuation.
    """
    # Use lowercase on lookup, trim spaces
    s = s.strip().lower()
    # Clean special names O'Leary MC KINLEY
    s = re.sub(r"\bO'([A-Z])",r'O\1', s)
    s = re.sub(r"\b(MC|LA|MAC) ([A-Z]{2,})",r"\1\2",s)
    s = re.sub(r"\s+",' ',s)
    # Replace punctuation, for now with space
    s = re.sub(r'\s*[\-\/,]+\s*',' ',s)
    return s


class Translator:

    def __init__(self,
                 filename:str   # translations.json
                 ):
        """
        Initialize a translator and load the translations.json definitions.
        """

        self.translations_by_en = {} # Translation for an English phrase
        self.translation_key_by_en = {} # Key (phrase_id) found
        self.translation_collisions = set() # English phrase with multiple translations
        # If multiple translations are present, then the self.translations_by_en
        # and self.translation_key_by_en is converted to a list.

        # The *_by_en dicts use lower case keys formed with form_translate_key()

        # Translations with {0} {1} etc are mapped to a list with
        # triple ((key, pat, format) with the translation key/phrase_id,
        # a compiled regex with groups matching {0}, ...
        # TODO: Oops, if English is out of order, convert to key
        self.translation_pats = [] # (key, pat, format) for substitutions

        #print(f"reading {filename}")
        self.translations_data = utils.read_json(filename)
        # translations_data is the root data, translations is the dict by key
        self.translations = self.translations_data['translations']

        self.translations_unmatched = set() # Unmatched translations found

        # Build the English->istr lookup tables and translate patterns
        for key, istr in self.translations.items():
            # Remove _desc so we can use the istr dict directly
            istr.pop('_desc',None)
            en = istr.get('en','').strip()
            if not en:
                continue
            enk = form_translate_key(en)

            params = istr.pop('_params',None)
            if not params:
                # Plain translation of full English phrase
                # Add to translations_by_en[] translation_key_by_en[]

                if enk not in self.translations_by_en:
                    # Save a unique translation
                    self.translations_by_en[enk] = istr
                    self.translation_key_by_en[enk] = key
                else:
                    # Collision
                    if enk not in self.translation_collisions:
                        # On initial collision convert to list
                        self.translation_collisions.add(enk)
                        self.translations_by_en[enk] = [ self.translations_by_en[enk] ]

                        self.translation_key_by_en[enk] = [self.translation_key_by_en[enk]]
                    self.translations_by_en[enk].append(istr)
                    self.translation_key_by_en[enk].append(key)

            else:
                # Create parametric translation
                # First form a list of regex to match for params
                parampat = [] # List of regex match for each param
                for p in params:
                    # find regex
                    rfound = r'*.?' # Map unmatched to anything
                    # Find a regex by matching the param name (HACK)
                    for (idpat, regex) in PARAM_NAME_REGEX:
                        if not idpat.search(p):
                            continue
                        rfound = regex
                        break
                    parampat.append(rfound)
                # Map the english
                # First convert special characters as-is
                patstr = re.sub(r'([^\w\{\}\- ])',r'\\\1',en)
                # TODO: We could also make matches independent of spaces, etc.
                #print(f"Regex for {key}={en} parampats={parampat}")
                try:
                    # Check the English for {0}...{len(params)-1}
                    i = 0
                    subs = re.findall(r'\{(\d+)\}',en)
                    for v in subs:
                        if int(v)!=i:
                            raise Exception("Invalid sequence for {key}:{en}")
                        i = i+1
                    if i!=len(parampat):
                        raise Exception("Invalid sequence for {key}:{en}")

                    # Convert {\d+} to parameter regex
                    patstr = re.sub(r'\{(\d+)\}',
                                lambda m:(r'('+parampat[int(m.group(1))]+')'),en)
                    pat = re.compile(f'^{patstr}$', flags=re.I)
                except:
                    # TODO: Handle this better
                    print(f"Invalid translation {key} en:{en}")
                    continue

                #print(f"Translation pat for {key} = {en} :{istr}")
                self.translation_pats.append((key, pat, istr))


    def lookup_phrase(self,
                      en:str,       # English phrase to translate
                      key_match:str=''  # If defined, regex to match istr key TODO
                      )->Dict:
        """
        Search for a translations entry to map an English phrase.
        Returns the istr dict or none.
        """

        enk = form_translate_key(en)
        if en in self.translations_unmatched:
            return None
        if enk in self.translation_collisions:
            # filter by key_match
            if not key_match:
                return None
            keys = [ key for key in self.translation_key_by_en[enk]
                     if re.search(key_match, key) ]
            if len(keys)!=1:
                # If we have a key_match, we could add to unmatched
                return None
            return self.translations[keys[0]]

        istr = self.translation_key_by_en.get(enk, None)
        #print(f"Lookup translation {en}={istr}")
        if istr:
            if key_match and enk in self.translation_key_by_en:
                # Validate the key pattern
                if not re.match(key_match, self.translation_key_by_en[enk]):
                    # Add to unmatched with required key pattern
                    self.translations_unmatched.add(f"{en} [key={key_match}]")
                    return None
            return istr

        # Search translation_pats
        for (key, pat, istr) in self.translation_pats:
            if key_match and not re.match(key_match, key):
                continue
            m = pat.match(en)
            if m:
                try:
                    newistr = { k:re.sub(r'\{(\d+)\}',
                                 lambda m2: m.group(int(m2.group(1))+1), v)
                        for k, v in istr.items()}
                    # Save in case of a repeat
                    self.translations_by_en[enk] = newistr
                    #print(f"Translation for {en} = {newistr}")
                    return newistr

                except:
                    # TODO: Handle this better
                    print(f"Translation pat error for {en}: {istr}")
                    break

        # Not found
        self.translations_unmatched.add(en)


    def __getitem__(self, key):
        """
        When the key for a translation is known, access it via []
        """
        v = self.translations.get(key)
        #print(f"translations[{key}]={v}")
        if not v:
            self.translations_unmatched.add(f"[key={key}]")
        return v

    def get(self,
            en:str,       # English phrase to translate
            key_match:str=''  # If defined, regex to match istr key TODO
            )->Dict:
        """
        Locate a translation or form an istr with english only.
        Converts a string to a istr Dict in all cases. If not
        found, the 'en' only is returned and the unmatched string recorded.
        """
        istr = self.lookup_phrase(en, key_match)
        if not istr:
            istr = {'en':en}
        return istr

    def check(self, istr:Dict)->Dict:
        """
        Checks for an English only istr, and performs a lookup
        if no translations are present.
        """
        en = istr.get('en',None)
        # Don't translate a number or single/double letter
        if not en or re.match(r'^(\d+|[A-Z]{,2})$', en):
            return istr
        have_translations = False
        for lang,v in istr.items():
            if v and lang!='en':
                have_translations = True
                break
        if not have_translations:
            newistr = self.lookup_phrase(en)
            if newistr:
                return newistr
        return istr

    def print_unmatched(self, filename):
        """
        If there were any unmatched translations, print them to filename
        """
        if self.translations_unmatched:
            with open(filename,'w') as outfile:
                for s in sorted(self.translations_unmatched):
                    print(s, file=outfile)






