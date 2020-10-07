# -*- coding: utf-8 -*-
#
# Copyright (C) 2019  Carl Hage
#
# This is free software: you can redistribute it and/or modify
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
Utilities for manipulating SHA256 checksums and files
"""
import hashlib, os, re, io, gzip

from typing import (Union, List, Pattern, Match, Dict, IO, AnyStr,
                    Optional, TextIO)
from dataclasses import dataclass
from zipfile import ZipFile

SHA_FILE_NAME = "sha256sum.txt"

UTF8_ENCODING = 'utf-8'

def load_sha_file(
    f:IO,       # opened IO object
    d:Dict[str,str]={}, # optional dict to append
    prefix:str=""       # file prefix to insert on filenames
    )->Dict[str,str]:   # Dictionary with (filename,hashstring) returned
    """
    Loads a SHA256SUM file, splitting the contents and returning a
    dictionary of hash strings by file name. The argument
    """
    linepat = re.compile(r'([0-9a-f]+)\s+\*?(.+)')
    for line in f:
        m = linepat.match(line.decode(UTF8_ENCODING).strip())
        if m:
            d[prefix+m.group(2)] = m.group(1)
    return d



def load_sha_filename(
    fn:str,             # File name to read
    d:Dict[str,str]={},  # optional dict to append
    prefix:str=""       # file prefix to insert on filenames
    )->Dict[str,str]:   # Dictionary with (filename,hashstring) returned
    """
    Loads a SHA256SUM file, splitting the contents and returning a
    dictionary of hash strings by file name. The argument
    """
    if os.path.isfile(fn):
        with open(fn, 'rb') as f:
            return load_sha_file(f,d,prefix)
    return None

def sha256sum_file(
    f:IO       # opened IO object (binary mode)
    )->str:
    """
    Computes a SHA256 hash by reading the file
    """
    h = hashlib.sha256()
    mv = memoryview(bytearray(65536))
    for n in iter(lambda:f.readinto(mv), 0):
        h.update(mv[:n])
    return h.hexdigest()

def sha256sum_filename(
    fn:str             # File name to read
    )->str:
    if os.path.isfile(fn):
        with open(fn, 'rb') as f:
            return sha256sum_file(f)
    return None

@dataclass
class SHAReader:
    """
    The SHAReader can be used to open a file and call readline() or
    readlines(). It can be used in a with statement to close the file.

    The path may contain a .zip file followed by a zip file member
    as if the zip file was a directory. If the path ends in .gz,
    the file will be unzipped
    """
    path:Union[str,io.IOBase]  # filename to read or io (binary)
    encoding:str=UTF8_ENCODING # Decode binary data
    binary_decode:bool=True    # True to decode binary input
    opener:Optional[open]=None # External opener to use with path

    def __enter__(self):
        # Check for a zipfile
        self.zipfile = None
        if isinstance(self.path, str) and not self.opener:
            m = re.match(r'^(.+\.zip)/(.+)', self.path)
            if m and os.path.isfile(m.group(1)):
                self.zipfile = ZipFile(m.group(1))
                self.path = m.group(2)
                self.opener = self.zipfile
        
        self.f = (self.opener.open(self.path) if self.opener else
                  self.path if isinstance(self.path, io.IOBase)
                  else gzip.open(self.path, 'rb')
                  if self.path.endswith(".gz")
                  else open(self.path, 'rb'))
        
        self.line_num = 0   # Reset a line counter
        self.hashcontext = hashlib.sha256()

        return self

    def __exit__(self, type, value, traceback):
        """
        Defines an context manager exit to so this can be used in a with/as
        """
        self.f.close()
        if self.zipfile:
            self.zipfile.close()
        self.f = None

    def __repr__(self):
        return f'<TSVReader {self.path}>'

    def hexdigest(self)->str:
        """
        Returns the SHA256 digest as a hex string
        """
        return self.hashcontext.hexdigest()

    def digest(self)->bytes:
        """
        Returns the SHA256 digest as binary bytes
        """
        return self.hashcontext.digest()

    def readline(self):
        """
        read a line and return the split line list.
        """
        self.line = self.f.readline()
        self.line_num += 1
        self.hashcontext.update(self.line)
        if self.binary_decode:
            self.line = self.line.decode(encoding=self.encoding)
        return self.line

    def readlines(self):
        """
        Interator to read a file and return the split line lists.
        """
        for line in self.f:
            self.line_num += 1
            self.hashcontext.update(line)
            if self.binary_decode:
                line = line.decode(encoding=self.encoding)
            self.line = line
            yield line


