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
Routines to load configuration files.
"""

import logging
import yaml
import re
import collections
import dataclasses
import inspect
import sys

from collections import namedtuple
from enum import Flag, auto
from io import IOBase
from typing import (List, Pattern, Match, Dict, NewType, Union, Callable,
                    Tuple, Any, NamedTuple, Set, IO, Optional)

DEFAULT_CONFIG_FILE_NAME = "config.yaml"

BOOL_TRUE_PAT = re.compile(r"^[yYtT1]")

# Flags to define default, errors, and warnings
class StrictFlags(Flag):
    NONE=0
    WARN_MISSING=auto()     # Imply null dict list or None, warn if missing
    ERROR_MISSING=auto()    # Fail for attributes without default missing
    WARN_EXTRA=auto()       # Warn extra attributes found
    ERROR_EXTRA=auto()      # Fail if an extra attribute is found
    WARN_INVALID=auto()     # Warn instead of fail on invalid parameter

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
    "stndrdth":r'(?:st|nd|rd|th)?',
    "any":r'.*',
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

def config_pattern(v:str)->Pattern:
    """
    The config_pattern function can be used in a type definition to
    convert a simple pattern string to compiled regex. This is
    a wrapper on reform_simple_regex
    """
    return reform_simple_regex(v)


def config_whole_pattern(v:str)->Pattern:
    """
    Similar to config_pattern except there is an implied ^pattern$
    to match a whole string.
    """
    return reform_simple_regex(v,True)


def config_pattern_list(l:Union[str,List[str]])->List[Pattern]:
    """
    The config_pattern_list converts a list of strings or single
    string split on newlines to a listof lines that form patterns
    of the form "string {variable} string" into a list of compiled
    regex values. The {variable} is converted into (.+?)  or
    a specific pattern in REGEX_NAMES with optional numeric suffix.
    """
    if isinstance(l, str):
        l = [l]
    return [reform_simple_regex(line)
                for s in l for line in s.split('\n')]

def config_whole_pattern_list(l:Union[str,List[str]])->List[Pattern]:
    """
    The config_pattern_list converts a list of strings or single
    string split on newlines to a list of lines that form patterns
    of the form "string {variable} string" into a list of compiled
    regex values. The {variable} is converted into (.+?)  or
    a specific pattern in REGEX_NAMES with optional numeric suffix.
    """
    if isinstance(l, str):
        l = [l]
    return [reform_simple_regex(line, True)
                for s in l for line in s.split('\n')]

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


def config_pattern_map(l:Union[str,List[str]])->PatternMap:
    """
    The config_pattern_map converts a set of lines that form patterns
    of the form "string {variable} string=repl {variable} repl"
    into a list of (regex, format) tuples.
    """
    if isinstance(l, str):
        l = [l]

    return [reform_regex_map(line)
        for s in l for line in s.split('\n')]



def config_whole_pattern_map(l:Union[str,List[str]])->Pattern:
    """
    Joins a list of simple patterns
    """
    if isinstance(l, str):
        l = [l]
    return [reform_regex_map(line, True)
        for s in l for line in s.split('\n')]



def config_idlist(v:str,        # Space separated id list
                 )->List[str]:  # Returned string set
    """
    Converts a string attribute value that is a space separated list of
    strings.
    """
    return str(v).split()


def eval_config_pattern(v:str,                  # Value to test
                        patlist:List[Pattern]   # List of patterns to try
                        )->Optional[Match]:               # Returns the Match found
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

def eval_config_pattern_remap(v:str,                  # Value to test
                            patlist:List[Pattern],  # List of patterns to try
                            )->str:      # Returns the formatted string or None
    """
    Loops over the list of pattern maps and returns the formatted map value
    """
    if not patlist or not v: return (None)
    for (pat,fmt) in patlist:
        m = pat.match(v)
        if m:
            return fmt.format_map(m.groupdict(""))
    return (None)

def validate_attr_ok(v:Any,     # Value to test
            validate,           # type annotation attribute validation
            k:str,              # Attribute path
        )->Any:                 # Returns updated value
    """
    Is a wrapper around validate_attr to raise an exception if there is
    a skip
    """
    (v, skip) = validate_attr(v, validate, k, True)
    if skip:
        raise InvalidConfig(f"Ignoring {k}, Expected {validate}")
    return v

def validate_attr(v:Any,        # Value to test
            validate,           # type annotation attribute validation
            k:str,              # Attribute path
            strict:StrictFlags=StrictFlags.NONE,  # exception/warn handlers
        )->Tuple[Any, bool]:  # Returns updated value and True to skip
    """
    Converts the raw string config file item to the correct type and validates
    the value. Returns (None, True) for an error, or (v, False) for validated v.
    """

    if v == None or not validate: return v, False     # Return None as-is

    # Some types are mapped in the local dict argtypeconv
    if isinstance(validate, collections.Hashable) and validate in argtypeconv:
        # Convert to string or other type
        validate = argtypeconv[validate]

    if inspect.isclass(validate):
        if issubclass(validate, List):
            if not isinstance(v,list):
                raise InvalidConfig(f"Expected {validate} for {k}")
            # get the type
            t = validate.__args__[0]
            v = [ validate_attr_ok(i, t, k) for i in v]
            return v, False

        if issubclass(validate, Set):
            if not isinstance(v,list):
                raise InvalidConfig(f"Expected {validate} for {k}")
            # get the type
            t = validate.__args__[0]
            v = set([ validate_attr_ok(i, t, k) for i in v])
            return v, False

        if issubclass(validate,Dict):
            if not isinstance(v,dict):
                raise InvalidConfig(f"Expected {validate} for {k}")
            # get the k,v types
            (kt, vt) = validate.__args__

            v = {validate_attr_ok(vk, kt, k):validate_attr_ok(vv, vt, k)
                for vk,vv in v.items()}
            return v, False

    if isinstance(validate, type) and hasattr(validate, "__annotations__"):
        if not isinstance(v,dict):
            raise InvalidConfig(f"Expected {validate} for {k}")
        # Use a recursive call to map a dict to dataclass instance
        return validate_attributes(validate, v, strict), False

    if callable(validate):
        # Validate is a function to check and convert
        try:
            v = validate(v)
        except:
            #logging.error(f"Invalid configuration file entry: {k}:{v} (callable)")
            return None, True
    elif isinstance(validate, Pattern):
        # Validate is a regex pattern to check a string value
        v = str(v)
        if not validate.search(v):
            #logging.error(f"Invalid configuration file entry: {k}:{v} (pattern)")
            return None, True
    else:
        # [TODO] No support for other types
         raise InvalidConfig(f"Unmatched configuration file definition: {k}:{validate}")

    return v, False

def validate_attributes(
        cls: type,                  # Class with @datatype annotations
        data: Dict[str,Any],        # (attribute_name:value) data to validate
        strict:StrictFlags=StrictFlags.NONE,  # exception/warn handlers
        ):
    """
    The members of the data dict are validated against the attributes
    defined in type annotations for the class. If additional keys are found,
    they are removed or an exception is raised in strict mode.
    """
    if cls==dict:
        # Assume a type of dict is valid
        return
    annotations = cls.__annotations__
    for k,v in list(data.items()):

        validator = annotations.get(k,None)
        if validator==None:
            if strict & StrictFlags.ERROR_EXTRA:
                raise InvalidConfig(f"Unknown configuration file entry: {k}")
            if strict & StrictFlags.WARN_EXTRA:
                logging.error(f"Unknown configuration file entry: {k}")
            data.pop(k)
            continue
        try:
            v, skip = validate_attr(v, validator, k, strict)
        except Exception as X:
            logging.error(f"Invalid configuration file entry: {k}:{X}")
            skip = True
        if skip:
            if not (strict & StrictFlags.WARN_INVALID):
                raise InvalidConfig(f"Invalid configuration file entry: {k}")
            data.pop(k)
        else:
            data[k] = v

    fields = cls.__dataclass_fields__
    # Create default for missing required fields
    for k,f in fields.items():
        #import pdb; pdb.set_trace()
        if (k in data or f.default_factory!=dataclasses.MISSING or
            f.default!=dataclasses.MISSING):
            continue
        if strict & StrictFlags.ERROR_MISSING:
            raise InvalidConfig(f"Missing configuration file entry: {k}")
        if strict & StrictFlags.WARN_MISSING:
            logging.error(f"Missing configuration file entry: {k}")

        data[k]= (None if not inspect.isclass(f.type) else
                  dict() if  issubclass(f.type,Dict) else
                  list() if issubclass(f.type,List) else
                  "" if issubclass(f.type,str) else None)


class Config:
    """
    This is an abstract class for converting yaml, json, or other data
    loaded into a dict into a dataclass object with type annotations and
    default values.

    A subclass of Config has class methods
    """

    @classmethod
    def create_from_dict(
            cls:type,                   # @dataclass to create
            data:Dict[str,Any],         # dictionary of (attribute_name:value)
            strict:StrictFlags=StrictFlags.NONE,  # exception/warn handlers
        )->object:                      # Instance of type cls
        """
        Reads the @dataclass type annotations defined in cls to validate
        the attributes in the data, casting types, then creating an instance
        of cls with the data as init parameters.
        """
        validate_attributes(cls, data, strict)
        try:
            return cls(**data)
        except TypeError as e:
            print("Missing configuration item:",e)
            raise(e)


    @classmethod
    def create_from_file(cls,
            config_file:Union[str,IO],      # File name or stream to read
            *config_default:Union[str,IO],  # Default file name(s)/stream to read
            loader:Callable[[IO],Dict[str,Any]]=yaml.safe_load, # file->dict
            strict:StrictFlags=StrictFlags.NONE,  # exception/warn handlers
        )->object:                          # Returns instance of cls
        """
        A shorthand wrapper combining create_from_dict(load_config_file(...))
        """
        data = cls.load_config_file(config_file, loader)
        for f in config_default:
            default = cls.load_config_file(f, loader)
            if not default:
                continue
            cls.overlay_data(data, default)

        return cls.create_from_dict(data)

    @classmethod
    def load_config_file(cls,
            config_file:Union[str,IO],      # File name or stream to read
            loader:Callable[[IO],Dict[str,Any]]=yaml.safe_load, # file->dict
        )->Dict[str,Any]:                    # Returns a dict loaded from the file
        """
        Invoke a loader to read an IO stream and return a dict, then
        validate that we have a dict data type.
        """
        try:
            if isinstance(config_file, IOBase):
                data = loader(config_file)
            elif config_file=='-':
                logging.info(f'Loading config data from stdin')
                data = loader(sys.stdin)
            elif not config_file:
                # Return empty dict if config_file is None or ''
                return dict()
            else:
                logging.info(f'Loading config data from {config_file}')
                with open(config_file) as f:
                    data = loader(f)
            if not data:
                # Return empty dict on no data
                return dict()
            # Verify the returned data is a dict
            if not isinstance(data, dict):
                logging.error(f"Invalid configuration file {config_file}: not a dictionary")
                return dict()
            return data
        except FileNotFoundError:
            # Use defaults if a config file is not present
            logging.info(f'Config file {config_file} not found')
            return dict()
        except yaml.YAMLError as exc_info:
            logging.error(f"Error in configuration file:{exc_info}")
            return dict()

    @classmethod
    def overlay_data(cls,
            data:Dict,      # Data to have default values added
            default:Dict    # Default data to overlay
        ):                  # No return value
        """
        The overlay_data copies values in the default dictionary to data
        if not defined.

        TODO: Handle dictionary and list object types that are mergable.
        For now dict types have a recursive overlay
        """
        for k,v in default.items():
            if k in data:
                if isinstance(v, dict) and isinstance(data[k], dict):
                    cls.overlay_data(data[k], v)
                continue
            data[k] = v
