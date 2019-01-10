# -*- coding: utf-8 -*-
#
# Copyright (C) 2018  Carl Hage
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
Utilities for candidate name processing
"""

# Library References
import logging
import re
import unicodedata

# Library imports
from nameparser import HumanName
from collections import OrderedDict
from typing import List, Pattern, Match, Dict, Any, NamedTuple

_log = logging.getLogger(__name__)

def cand_name_key(name:str)->str:
    """
    The candidate name is normalized into lower case ASCII characters,
    and all spacing and punctuation is removed. Unicode characters are
    converted into plain ascii a-z.
    """
    n = unicodedata.normalize('NFKD', name.casefold()) #.encode('ASCII', 'ignore')
    return re.sub('[^a-zA-Z0-9]','', n)

def cand_last_name_key(name:str)->str:
    """
    Return the lower cased ASCII key for a candidate last name
    A prefix ending with : can be used and included with an extracted last name,
    e.g. Measure:name, Retention:name
    """
    m = re.match(r'(.+:\s*)(.*)',name)
    if m:
        prefix, name = m.groups()
    else:
        prefix = ''
    last = HumanName(name).last
    if not last:
        last = name
    return cand_name_key(prefix+last)

def dict_append_list(d:dict,    # Dictionary
                     key: Any,  # Key for dictionary
                     val: Any): # Value in list
    """
    Helper to append the value to a list by key in the dictionary. Does
    not croak if d[key] is undefined.
    """
    if key not in d:
        d[key] = [ val ]
    else:
        d[key].append(val)

def dict_count_set(d:dict,    # Dictionary
                   key: Any,  # Key for dictionary
                   val: Any): # Values to count
    """
    Helper to count a set of values associated with akey in the dictionary.
    Initializes the dictionary entries
    """
    if key not in d:
        d[key] = { val:1 }
    elif val not in d[key]:
        d[key][val] = 1
    else:
        d[key][val] += 1

def check_mismatch(d:dict,    # Dictionary
                   key: Any,  # Key for dictionary
                   val: Any   # Value in list
                   )->bool:   # returns true on a mismatch
    """
    Helper to enter a value in a dictionary and compare mismatches on
    subsequent set for the same key
    """
    if key not in d:
        d[key] = val
    elif d[key] != val:
        return True
    return False


class CandContMatch:
    """
    The CandContMatch class can be used to match contest IDs or titles
    from 2 systems based on matching candidate names associated with the
    contest. Contest IDs with a set of candidate names are entered into
    a dictionary by candidate name. To match, candidate names and mapped
    contest IDs are entered. Then the name map can be resolved, and
    problems reported.

    A secondary lookup on candidate last name can be used to resolve any
    mismatches.
    """

    def __init__(self,
                 cont_idname: Dict[str,str]={},   # Convert contest_id to name
                 cont_id2name: Dict[str,str]={},  # Convert contest_id2 to name
                 question_pattern="yes|no|write-?in",  # Measure/Retention choices
                 debug:bool=False                 # Enable debug logging
                 ):
        """
        Creates the match object that holds the candidate name to contest ID
        mapping dictionaries.

        Two optional dictionaries are passed as parameters to convert the
        primary and secondary contest IDs into names.
        """
        self.cand_contlist = {}     # contest list by candidate name key
        self.cand2_contlist = {}    # contest2 list by candidate name key
        self.cand_contlist2 = {}    # contest list by candidate last name key
        self.cont_cands = {}        # candidate info by contest_id
        self.cont2_cands = {}       # candidate info by contest_id2
        self.cand_key2name = {}     # Convert key's to name for warnings
        self.cont_map = {}          # Contest ID mappings found
        self.cont_map2 = {}         # Inverse map contest_id -> contest_id2
        self.cand_map = {}          # Candidate ID mappings found
        self.cand_map2 = {}         # Inverse map candiate_id -> candidate_id2
        self.cont_maplist = {}      # Full name Contest ID mappings found
        self.cont_maplist2 = {}     # Last name Contest ID mappings found
        self.unmapped_candinfo = [] # cand_contlist candidate info not matched
        self.unmapped_candinfo2 = []# cand_contlist2 candidate info not matched
        self.unmapped_cont_ids = [] # unmapped contest_id
        self.unmapped_cont_id2s = []# unmapped contest_id2
        self.conflicts = []         # conflicting map messages
        self.warnings = []          # Warning messages, e.g. candidate spelling
        self.cont_idname = cont_idname
        self.cont_id2name = cont_id2name
        self.cand_idname = {}
        self.cand_id2name = {}
        self.question_choice_pat = re.compile(f"^({question_pattern})$", flags=re.I)
        self.debug = debug

    def cand_name_key(self, cand_name:str)->str:
        """
        Compute the case and unicode insensitive key, check for a mismatch
        with prior key, and generate a warning to note changed spellings.
        """
        key = cand_name_key(cand_name)
        if check_mismatch(self.cand_key2name, key, cand_name):
            self.warnings.append(
                F"Candidate spelling '{self.cand_key2name[key]}' > '{cand_name}'")
        return key

    def enter_contest(self,
                      is_master:bool,   # True for master contest def
                      contest_id:str,   # ID
                      contest_name:str, # Name
                      question:str=None,    # Question mapped to a name
                      ):
        """
        Enters the contest ID -> Contest name : cont_idname or cont_id2name
        for a contest, and if question is not null, use it as a pseudo
        candidate name. For measures, retention, recall, etc., the contest
        name should be mapped to a standard form with "type:" prefix.
        """
        if self.debug:
            _log.debug(f"enter_contest({contest_id},{contest_name},{question})")
        if is_master:
            self.cont_idname[contest_id] = contest_name
        else:
            self.cont_id2name[contest_id] = contest_name

        if question:
            if is_master:
                self.enter_cand(contest_id, "", question)
            else:
                self.lookup_cand(contest_id, "", question)

    def enter_cand(self,
                   contest_id:str,  # contest id/name
                   cand_id:str,     # candidate ID or null if ballot measure
                   cand_name:str):  # candidate full name
        """
        Define a contest ID associated with a candidate name
        """

        # Use a converted name for case and unicode insensitive matching
        key = self.cand_name_key(cand_name)
        if self.question_choice_pat.match(key):
            # Skip question and write-in choices
            return

        self.cand_idname[cand_id] = cand_name

        key2 = cand_last_name_key(cand_name)

        if self.debug:
            _log.debug(f"enter_cand({contest_id},{cand_id},{key},{key2})")
        # Save the arguments
        cand_info = {"contest_id": contest_id,
                     "cand_id": cand_id,
                     "key": key,
                     "last_key": key2,
                     "cand_name": cand_name}
        # Append all contest-candidate info to the lookup by name
        dict_append_list(self.cand_contlist, key, cand_info)
        # Append the info by contest ID
        dict_append_list(self.cont_cands, contest_id, cand_info)

        # Push a secondary lookup by last name
        dict_append_list(self.cand_contlist2, key2, cand_info)


    def set_contest_map(self,
                        contest_id2:str, # source contest id/name
                        contest_id:str): # target contest id/name
        """
        Enter a source->target mapping found. If there is a mismatch on the
        same key, an error message is appended.
        """
        if self.debug: _log.debug(f"set_contest_map({contest_id2},{contest_id})")
        if check_mismatch(self.cont_map, contest_id2, contest_id):
            self.conflicts.append(
f"Mismatch map for {contest_id2} -> {self.cont_map[contest_id2]} + {contest_id}")
        if check_mismatch(self.cont_map2, contest_id, contest_id2):
            self.conflicts.append(
f"Mismatch map2 for {contest_id} -> {self.cont_map2[contest_id]} + {contest_id2}")


    def set_cand_map(self,
                        cand_id2:str, # soource cand id/name
                        cand_id:str): # target cand id/name
        """
        Enter a source->target mapping found. If there is a mismatch on the
        same key, an error message is appended.
        """
        if check_mismatch(self.cand_map, cand_id2, cand_id):
            self.conflicts.append(
f"Mismatch candidate map for {cand_id2} -> {self.cont_map[cand_id2]} + {cand_id}")
        if check_mismatch(self.cand_map2, cand_id, cand_id2):
            self.conflicts.append(
f"Mismatch candidate map2 for {cand_id} -> {self.cont_map2[cand_id]} + {cand_id2}")


    def lookup_cand(self,
                   contest_id2:str, # contest id/name
                   cand_id2:str,    # candidate ID
                   cand_name:str):  # candidate full name
        """
        Compute the possible mappings of contest_id2 based on the candidate name
        """
        key = self.cand_name_key(cand_name)
        if self.question_choice_pat.match(key):
            # Skip question and write-in choices (Yes/No)
            return

        self.cand_id2name[cand_id2] = cand_name

        key2 = cand_last_name_key(cand_name)
        cand_info2 = {"contest_id":contest_id2,
                      "cand_id":cand_id2,
                      "key": key,
                      "last_key": key2,
                      "cand_name": cand_name}

        # Append the info by contest ID
        dict_append_list(self.cont2_cands, contest_id2, cand_info2)

        cont_list = self.cand_contlist.get(key,[])
        cont_list2 = self.cand_contlist2.get(key2,[])
        if not cont_list and not cont_list2:
            # This candidate name was not defined in enter_cand()
            # Save the unmapped candidate names by contest ID (?)
            # Disabled:
            #dict_append_list(self.unmapped_cands, contest_id2, cand_info2)
            return

        # We found one or more contests with a matching candidate name
        if self.debug:
            _log.debug(f"lookup_cand({key}={cont_list} {key2}={cont_list2}")
        if len(cont_list) == 1:
            # We found an unambiguous match
            # Set the contest map and check for mismatch
            contest_id = cont_list[0]["contest_id"]
            self.set_contest_map(contest_id2, cont_list[0]["contest_id"])
        elif len(cont_list2) == 1:
            contest_id = cont_list2[0]["contest_id"]
            self.set_contest_map(contest_id2, cont_list2[0]["contest_id"])

        # For each possible mapping, count the mappings
        for cand_info in cont_list:
            # Loop over IDs found and enter mapping count
            dict_count_set(self.cont_maplist, contest_id2,
                           cand_info["contest_id"])

        # Count possible mappings by last name
        for cand_info in cont_list2:
            dict_count_set(self.cont_maplist2, contest_id2,
                           cand_info["contest_id"])

        # End lookup_cand()

    def filter_found_mappings(self,
                              l:Dict[str,int] # List contest_id->count
                              )->bool:  # True on empty list
        """
        Filter a list of contest_ids with counts and remove entries already
        matched. If the list is empty, return true.
        """
        for contest_id in list(l.keys()) :
            if contest_id in self.cont_map2:
                l.pop(contest_id)
        return len(l)==0

    def get_unmapped(self)->bool:
        """
        Scan the list of contest_id and contest_id2 and compute the list
        of remaining unmatched IDs.

        Returns true if either all contest_id or contest_id2 values are matched
        """
        self.unmapped_cont_ids = [x for x in self.cont_cands.keys()
                                  if x not in self.cont_map2]
        self.unmapped_cont_id2s = [x for x in self.cont2_cands.keys()
                                  if x not in self.cont_map]
        return (len(self.unmapped_cont_ids)==0 or len(self.unmapped_cont_id2s)==0)

    def check_nocand_contests(self)->bool:
        """
        Extends the unmapped contests if there is no mapping and there is
        no contest info.

        Returns true if both all contest_id and contest_id2 values are matched
        """
        self.nocand_cont_ids = [x for x in self.cont_idname.keys()
                    if x not in self.cont_cands and x not in self.cont_map2]
        self.nocand_cont_id2s = [x for x in self.cont_id2name.keys()
                    if x not in self.cont2_cands and x not in self.cont_map]

        return (len(self.unmapped_cont_ids)==0 and
                len(self.unmapped_cont_id2s)==0 and
                len(self.nocand_cont_ids)==0 and
                len(self.nocand_cont_id2s)==0)

    def analyze_map(self, maplist:Dict):
        """
        Scan possible mappings
        """
        for majority_search in [False, True]:
            # Loop with basic search, then resolve mismatch on majority
            loop = True
            while (loop and len(maplist)>0):
                # Loop searching for new matches
                loop = False
                for contest_id2, l in list(maplist.items()):
                    # Loop over possible contest_id mappings

                    # l is a dictionary of contest_id with counts
                    # Fiter the list to remove already matched mappings
                    if self.filter_found_mappings(l):
                        maplist.pop(contest_id2)
                        continue

                    if len(l) == 1:
                        # Only one mapping remaining
                        found_contest = l.keys()[0]
                    elif majority_search:
                        # Look for one mapping with >1 and >others in count
                        max_count = 1
                        found_contest = ''
                        for contest_id, count in l.items():
                            if count > max_count:
                                max_count = count
                                found_contest = contest_id

                            elif count == max_count:
                               found_contest = ''
                    else:
                        continue
                    if found_contest:
                        self.set_contest_map(contest_id2, found_contest)
                        loop = True
                        # Remove this item
                        maplist.pop(contest_id2)


                # End loop over maplist.items
            # End loop repeating to find remaining unambiguous match
            if len(maplist)==0:
                # There are no remaining ambiguous matches
                return
        # End loop with majority_search

    # End analyze_map()

    def resolve_contests(self):
        """
        After the target IDs are entered and lookups performed, scan for
        new matches based on elimination.
        """

        # Check for all IDs mapped
        if self.get_unmapped(): return

        # Analyze remaining mappings based on last names
        self.analyze_map(self.cont_maplist)
        if self.get_unmapped(): return

        self.analyze_map(self.cont_maplist)
        if self.get_unmapped(): return

        # There are still unresolved contests

    def resolve_candidates(self):
        """
        Construct the candidate ID mappings
        """
        for contest_id2, l2 in self.cont2_cands.items():
            # Loop over contests to map
            #print("resolve_candidates",contest_id2)
            if contest_id2 not in self.cont_map:
                # No candidates match, don't append
                continue

            contest_id = self.cont_map[contest_id2]
            l = self.cont_cands[contest_id]
            found = 0
            for contest_info in l2:
                # Loop over candidates to map
                cand_id2 = contest_info['cand_id']
                if not cand_id2:
                    # Ballot measures can use candidate name as string with null id
                    # Mark special case to simulate a missing continue in outer loop
                    found = 10000
                    continue
                # Filter by full name key
                matches = ([i['cand_id'] for i in l
                           if i['key'] == contest_info['key'] ] or
                          [i['cand_id'] for i in l
                           if i['last_key'] == contest_info['last_key'] ])

                if len(matches) == 1:
                    self.set_cand_map(cand_id2, matches[0])
                    found += 1
                # We will resolve errors later by missing ID map
            # End Loop over candidates to map

            if found<len(l):
                # Filter the unmatched candidates in l
                self.unmapped_candinfo.extend(
                    [i for i in l if i['cand_id'] not in self.cand_map2])

            if found<len(l2):
                # Filter the unmatched candidates in l
                self.unmapped_candinfo2.extend(
                    [i for i in l2 if i['cand_id'] not in self.cand_map])

        # End Loop over contests to map
    # End resolve_candidates()
