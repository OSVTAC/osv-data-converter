#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Configuration file definition for processing EMS data
"""

from dataclasses import dataclass
from typing import Union, List, Pattern, Match, Dict, Tuple, Optional, Set
from config2 import (Config, StrictFlags, config_pattern, InvalidConfig,
                     config_idlist, )

# Extended types
DistrictId = str                # ID Code for a district
DistrictIdPrefix = str          # Partial District ID Code
PartyId = str                   # Party ID
RandomAlphabetName = str        # Randomized alphabet name
RandomAlphabet = str            # 26 Letter randomization of A-Z
DistrictClassification = str    # District Classification Code
ColumnNames = str               # Space separated source file column header
TranslateTable = Dict[int,int]  # Table matching ord(character)


def eval_prefix_map(v:str,                      # Code value to check
                    patlist:Dict[str,str]       # Dictionary of (prefix,retval)
                    )->str:                     # Returns value found or None
    """
    Locates the value in a dictionary where the key is a prefix
    for the string to match v.
    """
    for (pat,retval) in patlist.items():
        if v.startswith(pat):
            return retval
    return None

def random_alphabet(v:str)->Optional[TranslateTable]:
    """
    Convert a randomized alphabet to a translate table
    """
    if not v:
        return None
    if not isinstance(v, str) or len(v)!=26:    
        raise InvalidConfig(f"Invalid Random Alphabet '{v}'")
    v = v.upper()
    return(v,'ABCDEFGHIJKLMNOPQRSTUVWXYZ')

CONFIG_FILE = "config-ems.yaml"


@dataclass
class EMSConfig(Config):
     # Map district code prefix to classification:
    district_code_classification: Dict[DistrictIdPrefix,DistrictClassification]
    # List of classification for results summary:
    summary_district_classifications: List[DistrictClassification]

    # For extracting poll location address
    poll_location_columns: ColumnNames             # Column ID(s) for poll location/site name
    poll_directions_columns: ColumnNames           # Column ID(s) for location details
    poll_accessibility_columns: ColumnNames        # Column ID(s) for poll wheelchair access
    poll_address_columns: ColumnNames              # Column ID(s) for poll street address
    poll_city_columns: ColumnNames                 # Column ID(s) for city, state, zip
    poll_vote_by_mail_pattern: config_pattern      # Name pattern matching vote by mail
    poll_vote_by_mail_column: ColumnNames          # Column with poll_vote_by_mail_pattern

    # For adjustment of the voting district for countywide contests:
    contest_district_map:Dict[DistrictId,List[config_pattern]]

    # For extracting content from EMS reports:
    party_heading_map: Dict[PartyId, str]       # Party ID to header map
    party_id_order: config_idlist               # Party ID order
    no_voter_precincts: config_idlist           # Precincts to ignore

    # For computation of candidate rotations
    # Alphabet A-Z for each random alphabet used:
    RandomAlphabets: Dict[RandomAlphabetName,random_alphabet]
    # List of contest name patterns for each random alphabet:
    RandomAlphabet_contests: Dict[RandomAlphabetName,List[config_pattern]]
    default_alphabet: RandomAlphabetName          # Name of the alphabet used if not matched
    # Class used for rotation number with list of voting district classifications
    candidate_rotation_districts: Dict[DistrictClassification,List[DistrictClassification]]

    bt_digits: int=3                                # Number of digits in a ballot type
    contest_digits: int=3                           # Number of digits in a contest ID
    tsv_separator: str='|'                          # Separator for tsv files

    def map_contest_district(self,
            contest_name: str,          # Contest name to match
            district_id: str,           # Unmapped ID
            ):
        """
        Checks the contest_district_map for an equivalent to the whole county
        district code by searching the list of patterns on the contest name.
        """
        for new_code, patlist in self.contest_district_map.items():
            for pat in patlist:
                if pat.search(contest_name):
                    return new_code
        return district_id


def load_ems_config():
    return EMSConfig.create_from_file(CONFIG_FILE, 
                  strict=StrictFlags.WARN_EXTRA|StrictFlags.WARN_INVALID)
