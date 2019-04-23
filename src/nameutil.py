#!/usr/bin/env python3
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
Utilities for converting case in titles and names.
"""

from titlecase import titlecase
from typing import Union, List, Pattern, Match, Dict

# TODO: Fix these
capwords = {'II','III'}

def conv_title_case(s:str)->str:
    """
    Wrapper for titlecase that fixes any problems in the standard library
    """
    s = titlecase(s)

    return s

def uc2_title_case(s:str)->str:
    """
    Convert case only if all capitals
    """
    if s.isupper():
        s = conv_title_case(s)
    return s

def conv_name_case(s:str)->str:
    """
    Convert uppercase or lower case names to proper capitalization
    """
    s = titlecase(s)
    return s

def uc2_name_case(s:str)->str:
    """
    Convert uppercase only names to proper capitalization
    """
    if s.isupper():
        s = conv_name_case(s)
    return s
