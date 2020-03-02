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
import hashlib, os, re
from typing import Union, List, Pattern, Match, Dict, IO, AnyStr

SHA_FILE_NAME = "sha256sum.txt"

ENCODING = 'utf-8'

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
        m = linepat.match(line.decode(ENCODING).strip())
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


