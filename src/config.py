#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2019  Carl Hage
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
Routines to load configuration files.
"""

import logging
import yaml
import re

from collections import namedtuple
from typing import List, Pattern, Match, Dict, NewType, Tuple, Any, NamedTuple

DEFAULT_CONFIG_FILE_NAME = "config.yaml"

BOOL_TRUE_PAT = re.compile(r"^[yYtT1]")

class InvalidConfig(Exception):pass # Error: invalid config file entry

def anytobool(v:any)->bool:
    """
    Convert the type to boolean. None is preserved. String gets converted
    "" as None, YT1 as True, others as False.
    """
    if v == None: return v

    if isinstance(v, str):
        # For string return None for "", or true if starting with [TY1]
        if len(v)<1: return None
        return bool(BOOL_TRUE_PAT.match(v))

    return bool(v)

def compileRegex(v:str)->Pattern:
    """
    Compile a regex pattern
    """
    if v == None: return v
    return re.compile(v)


argtypeconv = {
    bool: anytobool,
    str: str,
    Pattern: compileRegex
    }

# PatternMap is for substitutions with a tuple of pattern with format str
PatternMap = NewType('PatternMap', Tuple[Pattern,str])

# We define specific patterns
REGEX_NAMES = {
    "number":r'\d+',
    "digit":r'\d',
    "letter":r'[a-z]',
    "letterxx":r'[a-z][a-z]?',
    "word":r'[a-z]+',
    "stndrdth":r'(?:st|nd|rd|th)?'
    }


def reform_simple_regex(v:str,              # Base pattern
                        whole:bool=False    # Matches the whole string
                        )->Pattern:         # Returns a compiled pattern
    """
    Converts the simplified pattern "string {variable} string"
    to a python regex with "string (?P<variable>.*?)
    """
    # Put a \ before characters other than space, word, {}-
    v = re.sub(r'([^\w\{\}\- ])',r'\\\1',v)
    # Handle ^ and $ at the begin/end as-is
    v = re.sub(r'^\\\^','^',v)
    v = re.sub(r'\\\$$','$',v)
    if whole:
        v = '^'+v+'$'
    # Convert beginning and ending words to \bword
    v = re.sub(r'^(\w)',r'\\b\1',v)
    v = re.sub(r'(\w)$',r'\1\\b',v)
    # Convert {name} to (?P<name>.+?) or (?P<name>regex)
    v = re.sub(r'\{([a-zA-Z_]+)(\d*)\}',
               lambda m: (r'(?P<'  + m.group(1) + m.group(2) + '>' +
                             REGEX_NAMES.get(m.group(1),r'.+?') + ')'),v)
    #logging.debug(f"Found config_pattern '{v}'")
    return re.compile(v, flags=re.I)


def reform_regex_map(v:str,                 # Base pattern
                     whole:bool=False       # Matches the whole string
                     )->PatternMap:         # Returns a (pattern,repl) tuple
    """
    Converts a string in the form "string {variable} string=repl {variable} repl"
    into a tuple of a compiled regex match with named patterns and
    a split format string.
    """
    m = re.match(r'^(.*?)=(.*)', v)
    if not m:
        raise InvalidConfig(f"Invalid Config Pattern Map '{v}'")
    pat, fmt = m.groups()
    # Validate {name} found in fmt against those in pat
    namesfound = set(re.findall(r'\{\w+\}',pat))
    for n in re.findall(r'\{\w+\}',fmt):
        if n not in namesfound:
            raise InvalidConfig(f"Invalid Config Pattern Map '{v}' {n} not found")
    return (reform_simple_regex(pat, whole), fmt)


def config_pattern_list(l:List[str])->List[Pattern]:
    """
    The config_pattern_list converts a set of lines that form patterns
    of the form "string {variable} string" into a list of compiled
    regex values. The {variable} is converted into (.+?)  or
    a specific pattern in REGEX_NAMES with optional numeric suffix.
    """
    return list(map(reform_simple_regex, l))


def config_whole_pattern_list(l:List[str])->List[Pattern]:
    """
    Similar to config_pattern_list except there is an implied ^pattern$
    to match a whole string.
    """
    return [reform_simple_regex(i,True) for i in l]


def config_pattern_map(l:List[str])->List[PatternMap]:
    """
    The config_pattern_list converts a set of lines that form patterns
    of the form "string {variable} string=repl {variable} repl"
    into a list of (regex, format) tuples.
    """
    return list(map(reform_regex_map, l))


def config_pattern_map_dict(d:Dict[str,List[str]])->Dict[str,List[PatternMap]]:
    """
    Processes a dictionary of config_pattern_map
    """
    for k in d.keys():
        d[k] = config_pattern_map(d[k])
    return d


def config_whole_pattern_map(l:List[str])->Pattern:
    """
    Joins a list of simple patterns
    """
    return [reform_regex_map(i,True) for i in l]


def config_strlist_dict(d:Dict[str,List[str]])->Dict[str,List[str]]:
    """
    Joins a list of simple patterns
    """
    if d is None:
        d = {} # Default is an empty dict
    elif not isinstance(d,dict):
        raise InvalidConfig(f"Invalid Config Dictionary")
    else:
        # Validate each dict entry as list of str
        for k,v in d.items():
            if v is None:
                d[k] = v = []
            if not isinstance(k,str) or not isinstance(v,list):
                raise InvalidConfig(f"Invalid Config Entry")
            for i in v:
                if not isinstance(i,str):
                    raise InvalidConfig(f"Invalid Config Entry")

    return d


def eval_config_pattern(v:str,                  # Value to test
                        patlist:List[Pattern]   # List of patterns to try
                        )->Match:               # Returns the Match found
    """
    Loops over the list of patterns and returns the first match found
    """
    if not patlist: return None

    for i in patlist:
        m = i.search(v)
        if m:
            return m
    return None

def eval_config_pattern_map(v:str,                  # Value to test
                            patlist:List[Pattern],  # List of patterns to try
                            first_match=False       # True to return first matched
                            )->Tuple[str,int]:      # Returns the string and map count
    """
    Loops over the list of pattern maps and performs substitutions on
    the string with  patterns found.
    """
    if not patlist or not v: return (v, 0)

    count = 0


    for (pat,fmt) in patlist:
        (v, n) = pat.subn(lambda m: fmt.format_map(m.groupdict("")), v)
        if first_match and n:
            return (v, n)
        count += n
    return (v, count)

def setDefault(d:dict,          # Dict to set
               keys:List[str]   # Valid attributes
               ):
    """
    Sets a default value of None for keys in valid_attrs missing in d
    """
    if not keys: return      # Skip if there are no keys

    for k in keys:
        if k not in d:
            d[k] = None


def validate_attr(v:Any,         # Value to test
                  validate,     # attribute validation
                  k:str         # Attribute path
                  )->Tuple[Any, bool]:  # Returns updated value and True to skip
    """
    Converts the raw string config file item to the correct type and validates
    the value. Returns (None, True) for an error, or (v, False) for validated v.
    """
    if v == None or not validate: return v, False     # Return None as-is

    if callable(validate):
        # Validate is a function to check and convert
        try:
            v = validate(v)
        except:
            logging.error(f"Invalid configuration file entry: {k}:{v} (callable)")
            return None, True
    elif isinstance(validate, Pattern):
        # Validate is a regex pattern to check
        if not validate.search(v):
            logging.error(f"Invalid configuration file entry: {k}:{v} (pattern)")
            return None, True
    elif isinstance(validate, list):
        # Validate a list of items
        if len(validate) != 1:
            logging.error(f"Invalid validation list for {k}")
            return None, True
        validate = validate[0]
        if isinstance(v, dict):
            v = [ v ] # Convert single item to a list
        i = 0   # Counter for index
        def formKeyIndex(k:str)->str:
            # Append [i]
            nonlocal i
            k = f"{k}[{i}]"
            i = i+1
            return k
        v = [ newv for (newv, isErr) in
              [ validate_attr(nextv, validate, formKeyIndex(k)) for nextv in v]
              if not isErr]
    elif isinstance(validate, tuple) and getattr(validate,"_fields",None):
        # Namedtuple values
        if not isinstance(v, dict):
            logging.error(f"Invalid configuration file entry: {k}:{v} (dict)")
            return None, True
        # Set a default for all keys
        args = [ newv if not isErr else None for (newv, isErr) in
             [validate_attr(v.get(subk,None), nextval, f"{k}.{subk}")
              for (subk, nextval) in validate._asdict().items()]]
        v = type(validate)(*args)
        #print(k,v)

    elif validate in argtypeconv and v!=None:
        # Convert to string or other type
        v = argtypeconv[validate](v)
    else:
        # [TODO] No support for other types
         logging.error(f"Unmatched configuration file entry: {k}:{validate}")
         exit(0)
         pass
    return v, False

class Config:

    """
    [TODO]
    """

    def __init__(self,
                 config_file_name:str=DEFAULT_CONFIG_FILE_NAME, # File to load
                 root_config_file_name:str=None,    # System-wide defaults
                 valid_attrs:Dict=None,             # List of attributes
                 default_config:Dict=None,          # Built-in defaults
                 debug=False):  # Print debug log of data found
        """
        Args:
          config_file_name: Name of YAML configuration file to load
          root_config_file_name: Default value and system-wide settings
        """

        self.config_file_name = config_file_name
        self.valid_attrs = valid_attrs;
        self.debug = debug

        if root_config_file_name:
            # For system-wide default values and
            root_config = self.load_config_file(root_config_file_name)
        else:
            root_config = None

        local_config = self.load_config_file(config_file_name);

        # Compute the config search path
        # [TODO]

        self.overlay_config(local_config)
        if root_config:
            self.overlay_config(root_config)
        if default_config:
            self.overlay_config(default_config)
        self.finalize_config()

    def load_config_file(self,filepath:str):
        """
        Loads the parsed contents of the specified file.

        Returns: the dictionary of parsed data or None if not present or invalid

        Args:
          filepath: Full file path to load
        """

        logging.info(f'Loading config data from {filepath}')
        try:
            if filepath=='-':
                config = yaml.safe_load(sys,stdin)
            else:
                with open(filepath) as f:
                    config = yaml.safe_load(f)
            # Verify the returned data is a dict
            if not isinstance(config, dict):
                logging.error(f"Invalid configuration file {filepath}: not a dictionary")
                return None
            if self.debug:
                logging.debug("yaml load:",config)
            return config
        except FileNotFoundError:
            logging.debug(f'Config file {filepath} not found')
            return None
        except yaml.YAMLError as exc_info:
            logging.error("Error in configuration file:",exc_info)
            return None

    def overlay_config(self,newconfig:dict,replace:bool=False):
        """
        Overlays a configuration dict into the configuration data,
        either replacing any existing data (a higher level config
        replaces pre-loaded default data) or setting values only
        if not already defined (the new config provides defaults).

        Each item in newconfig is validated and possibly converted
        from a string format.

        [For future] Some config attributes might be prepended or appended,
        as defined in the config schema.

        Args:
          newconfig:    Parsed configuration data dict or None to skip
          replace:      If true, replace defined entries, otherwise not

        """

        if newconfig is None: return;

        setDefault(newconfig, self.valid_attrs.keys())

        for k,v in newconfig.items():
            if not replace and hasattr(self,k) and self[k]!=None: continue
            if self.valid_attrs:
                if k not in self.valid_attrs: continue
                (v, isErr) = validate_attr(v, self.valid_attrs[k], k)
                if isErr: continue

            if self.debug:
                logging.debug(f'set Config.{k}={v}')
            setattr(self,k,v)

    def overlay_config_file(self,filepath:str,replace=False):
        """
        Shorthand combination of load_config_file() and overlay_config()
        """
        self.overlay_config(self.load_config_file(filepath),replace)

    def overlay_config_path(self,searchpath,filename:str,replace=False):
        """
        Overlay configuration files found in the search path
        """
    def finalize_config(self):
        """
        Validate config data after loading.
        """
        if self.valid_attrs:
            # Set missing values to None
            for k in self.valid_attrs.keys():
                if not hasattr(self,k):
                    setattr(self,k,None)


