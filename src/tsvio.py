# -*- coding: utf-8 -*-
#
# Open Source Voting Results Reporter (ORR) - election results report generator
# Copyright (C) 2018  Carl Hage
# Copyright (C) 2018  Chris Jerdonek
#
# This file is part of Open Source Voting Results Reporter (ORR).
#
# ORR is free software: you can redistribute it and/or modify
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
Subroutine definitions and reader class to process delimited text files.
Data represented in unquoted tab (\t), pipe (|), or comma-delimited files
can be read and parsed using routines in this module. The reader can
optionally automatically determine the delimiter used, and save the
header array. No quotes are used, so to represent newlines and the
delimiter character, any newline or delimiter characters in field strings
are mapped to/from UTF-8 substitutes.

When reading lines and splitting into a string array, trimmed delimiters
at the end of the line will read as null strings for the full width
defined by a header line.

A TSVReader object maintains a context with delimiter, line number,
number of columns, and header name array.

This module does not use general libraries to avoid unneeded complexity.

TSV writing capability will be added later.
"""

import re
import io
from typing import Dict, Tuple, List, TextIO, Union
from collections import OrderedDict
import logging

UTF8_ENCODING = 'utf-8'

#--- Constants to map characters

TSV_SOURCE_CHAR_MAP = '\n\t'
TSV_FILE_CHAR_MAP = '␤␉'

PSV_SOURCE_CHAR_MAP = '\n|'
PSV_FILE_CHAR_MAP = '␤¦'

CSV_SOURCE_CHAR_MAP = '\n,'
CSV_FILE_CHAR_MAP = '␤，'

map_tsv_data = str.maketrans(TSV_SOURCE_CHAR_MAP,TSV_FILE_CHAR_MAP)
unmap_tsv_data = str.maketrans(TSV_FILE_CHAR_MAP,TSV_SOURCE_CHAR_MAP)

map_psv_data = str.maketrans(PSV_SOURCE_CHAR_MAP,PSV_FILE_CHAR_MAP)
unmap_psv_data = str.maketrans(PSV_FILE_CHAR_MAP,PSV_SOURCE_CHAR_MAP)

map_csv_data = str.maketrans(CSV_SOURCE_CHAR_MAP,CSV_FILE_CHAR_MAP)
unmap_csv_data = str.maketrans(CSV_FILE_CHAR_MAP,CSV_SOURCE_CHAR_MAP)

class DuplicateError(Exception):pass # Error: mismatched duplicate

_log = logging.getLogger(__name__)

#--- Field manipulation routines

def split_line(
        line:str,       # line to be split into fields
        sep:str='\t',   # delimiter separating fields
        trim:str='\r\n'     # end characters to strip, default is \r\n
        ) -> List[str]:  # Returns mapped field list
    """
    Removes trailing whitespace, splits fields by the delimiter character,
    then returns a list of strings with unmapped line end and delimiter
    character translations.
    """
    # Optional end of line strip
    if trim != None: line = line.rstrip(trim)

    if sep == '\t':
        mapdata = unmap_tsv_data
    elif sep == '|':
        mapdata = unmap_psv_data
    elif sep == ',':
        mapdata  = unmap_csv_data
    else:
        mapdata = None

    return [f.translate(mapdata) if mapdata else f for f in line.split(sep)]


class TSVReader:

    """
    The TSVReader class maintains header, delimiter, linecount and
    routines to read and convert delimited text lines.

    Attributes:
        f:          file object read
        sep:        delimiter separating fields
        header:     list of header strings
        headerline: header joined into a string with "|" separators
        num_columns: number of fields in the header or 0 if not read
        line_num:   line number in file
    """

    def __init__(self,
                 path:str,              # filename to read
                 sep:str=None,          # delimiter separating fields
                 read_header:bool=True, # Read and save the header
                 encoding:str=UTF8_ENCODING,
                 binary_decode:bool=False,  # True for external decode
                 opener=None,               # External opener
                 validate_header:str=None): # Expected header
        """
        Creates a tsv reader object. The opened file is passed in as f
        (so a with/as statement can provide a file open context).
        If read_header is true, the first line is assumed to be a
        header. If the sep column separating character is not supplied,
        the characters '\t|,' will be searched in the header line (if
        read) to automatically set the separator, otherwise tab is assumed.

        Args:
          path: the path to open, as a path-like object.
        """
        self.path = path
        self.sep = sep
        self.read_header = read_header
        self.encoding = encoding
        self.opener = opener
        self.binary_decode = binary_decode
        self.validate_header = validate_header

    def __enter__(self):
        self.f = (self.opener.open(self.path) if self.opener else
                  self.path if isinstance(self.path, io.IOBase)
                  else open(self.path, encoding=self.encoding))
        if self.read_header:
            # The first line is a header with field names and column count
            line = self.f.readline()
            if self.binary_decode:
                line = line.decode(self.encoding)
            self.line_num = 1   # Reset a line counter
            if self.sep is None:
                # derive the delimiter from characters in the header
                for c in '\t|,':
                    if c in line:
                        self.sep = c
                        break
            if self.sep is None:
                raise RuntimeError(f'no delimiter found in the header of {self.path}')
            self.header = split_line(line,self.sep)
            self.headerline = "|".join(self.header)
            self.num_columns = len(self.header)
            if self.validate_header and self.headerline != self.validate_header:
                raise RuntimeError(f"Mismatched header in {self.path}:\n   {self.headerline}\n!= {self.validate_header}")

        else:
            if self.sep is None:
                self.sep = '\t' # default delimiter is a tab
            self.num_columns = 0    # 0 means no column info
            self.line_num = 0   # Reset a line counter

        return self

    def __exit__(self, type, value, traceback):
        """
        Defines an context manager exit to so this can be used in a with/as
        """
        self.f.close()
        self.f = None

    def __repr__(self):
        return f'<TSVReader {self.path}>'

    def convline(self,line:str) -> List[str]:
        """
        Convert a line and return a list of strings for each
        field. If fewer columns are found than defined in the header,
        the list is extended with null strings. (If whitespace is trimmed
        from a line, the missing \t get mapped to null strings.)
        """
        if self.binary_decode:
            line = line.decode(self.encoding)

        l = split_line(line,self.sep)
        if len(l) < self.num_columns:
            # Extend the list with null strings to match header count
            l.extend([ '' ] * (self.num_columns - len(l)))
        return l

    def readline(self):
        """
        read a line and return the split line list.
        """
        self.line = self.f.readline()
        self.line_num += 1
        return self.convline(self.line)

    def readlines(self):
        """
        Interator to read a file and return the split line lists.
        """
        for line in self.f:
            if line.strip() == '':
                continue
            self.line = line
            self.line_num += 1
            yield self.convline(line)

    def find_header_column(self,
                           col_name:str # Name of the column
                           )->int:      # Returns the column index or None
        try:
            return self.header.index(col_name)
        except:
            return None

    def readdict(self):
        """
        Iterator to return a dict of values by header field name
        """
        for l in self.readlines():
            yield dict(zip(self.header,l))

    def loaddict(self,
                 keycolumn=0    # Column for ID
                 )->Dict:
        """
        Load lines of the file into a dictionary by key column with readdict hash
        """
        h = {}
        for l in self.readlines():
            key = l[keycolumn]
            if key in h:
                global _log
                _log.error(f"Duplicate key '{key}' at {self.path}:{self.line_num}")
                continue;
            h[key] = dict(zip(self.header,l))
        return h

    def load_simple_dict(self,
                         keycolumn:Union[int,str]=0,    # Column for ID
                         valcolumn:Union[int,str]=1     # Column for value
                         )->Dict[str,str]:
        """
        Load lines of the file into a dictionary by key column 0 value column 1
        """
        h = {}
        if isinstance(keycolumn,str):
            keycolumn = self.header.index(keycolumn)
        if isinstance(valcolumn,str):
            valcolumn = self.header.index(valcolumn)
        for l in self.readlines():
            key = l[keycolumn]
            if key in h:
                _log.error(f"Duplicate key '{key}' at {self.path}:{self.line_num}")
                continue;
            h[key] = l[valcolumn]
        return h

class TSVWriter:

    """
    The TSVWriter class maintains header, delimiter, linecount and
    routines to read and convert delimited text lines.

    Attributes:
        f:          file object written
        sep:        delimiter separating fields
        header:     list of header strings
    """
    def __init__(self,
                 path:str,              # filename to write
                 sort:bool=True,        # true if lines are sorted
                 sep:str='\t',          # delimiter separating fields
                 header:str=None,       # | separated header
                 unique_col_check:int=None,   # column to insure unique value
                 strip_trailing_sep:bool=True,   # strip blank trailing columns
                 map_data=None,         # str.maketrans() map
                 encoding:str=UTF8_ENCODING):
        """
        Creates a tsv writer object. Lines can be written directly to the
        file, or if the sort option is True, lines are first collected in
        a list, then sorted. The file is then written when the context is
        exited. If the unique_col_check is provided, it is the index of
        the column that contains an ID that must be unique. If not it will
        raise the DuplicateError exception.

        Args:
          path: the path to open, as a path-like object.
        """
        self.path = path
        self.sep = sep
        self.header = header
        self.sort = sort
        self.encoding = encoding
        self.unique_col_check = unique_col_check
        self.lines = []
        self.linedict = OrderedDict()
        self.strip_trailing_sep = strip_trailing_sep
        self.map_data = map_data


    def __enter__(self):
        self.f = open(self.path, "w", encoding=self.encoding)
        # Write a header if defined
        if self.header:
            if self.sep != "|":
                self.header = re.sub(r'\|', self.sep, self.header)
            self.f.write(self.header+'\n')
        return self

    def __exit__(self, type, value, traceback):
        """
        Defines an context manager exit to so this can be used in a with/as
        """
        if self.lines:
            if self.sort:
                self.lines = sorted(self.lines)
            self.f.writelines(self.lines)
        self.f.close()
        self.f = None

    def __repr__(self):
        return f'<TSVWriter {self.path}>'

    def joinline(self,
                 *args)->str:
        """
        Join a list of columns with \t and append \n
        """
        args = [s.translate(self.map_data) if self.map_data else s
                for s in map(str,args)]
        line = self.sep.join(args)
        if self.strip_trailing_sep:
            line = line.rstrip(self.sep)
        return(line+'\n')

    def addline(self,
                *args       # Argument list is converted to strings
                )->str:     # Returns the joined line
        """
        Join a list of columns with \t and append \n, then add to the line list

        Returns the line constructed.

        If the unique_col_check is defined, then the arg[unique_col_check]
        is used as a key that must have a new or matching value, or
        else the DuplicateError exception is raised. The self.found is
        set if there is a prior match.
        """
        line = self.joinline(*args)
        if self.unique_col_check != None:
            # Check for uniqueness
            key = args[self.unique_col_check]
            self.found = key in self.linedict
            if self.found:
                self.priorline = self.linedict[key]
                if self.priorline != line:
                    raise DuplicateError(
            f"Mismatched {self.path} duplicate\n   {self.priorline}!= {line}")
                return(line)
            else:
                # Save the line
                self.linedict[key] = line
        if self.sort:
            # Save lines to sort later
            self.lines.append(line)
        else:
            # Otherwise write out the line immediately
            self.f.write(line)
        return(line)

