#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2018  Carl Hage
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
Wrapper for regular expression processing that provides some of the
conviences of perl, e.g. substitutions with returned match values and
the ability to perform regex matches or substitutions using "or" expressions.
"""

import re
from typing import Union, List, Pattern, Match, Dict

def strnull(x:str)->str:
    """
    Maps None to ""
    """
    return "" if x == None else x

class re2:
    """
    The re2 is a wrapper on re to maintain a single context for
    search and results. This somewhat compensates for python's feeble
    regex support. Allows tests for search results without having
    to repeat the search to get the patterns. Allows substitutions with
    retained match groups.

    Partially allows similar functionality possible in perl
    """

    def __init__(self,
                 pattern:str,   # Regex pattern to compile
                 flags=0):      # Flags for compile
        """
        Creates an object to hold a compiled regex and results match.
        """
        self.regex = re.compile(pattern, flags)
        self.m = None
        self.nsubs = 0

    def search(self, string:str)->Match:
        """
        Invokes the re.search but saves the match object in self.m. The
        returned value can be used with an if statement or "or/and" expression.
        """
        self.m = self.regex.search(string)
        return self.m

    def match(self, string:str)->Match:
        """
        Invokes the re.match but saves the match object in self.m. The
        returned value can be used with an if statement or "or/and" expression.
        """
        self.m = self.regex.match(string)
        return self.m

    def sub(self,
            repl:str,           # Replacement string
            string:str,         # String to search and modify
            count=0,            # Limit on repeated substitutions
            pattern:Union[str,Match]=None, # Optional new pattern to match
            flags=0,            # Replacement flags
            formatted=False     # If "repl" is formatted {0},{1} with match groups
            )->str:             # Returns "string" modified by matched repl
        """
        Like re.sub() but saves the last match object in self.m
        Use formatted {0} {1} instead of \1 \2 etc.
        """
        if pattern is None:
            pattern = self.regex

        def rfunc(m):
            self.m = m
            if formatted:
                args = map(strnull,m.groups())
                return repl.format(*args)
            return repl

        self.m = None
        (self.string, self.nsubs) = re.subn(pattern, rfunc, string, count, flags)
        return self.string

    def subt(self, repl, string, count=0):
        """
        Like re.sub, but saves the last match and string, returns nsubs
        """
        self.sub(repl, string, count)
        return self.nsubs

    def sub2(self, string, pattern, repl, count=0, flags=0):
        """
        Wrapper for self.sub with string, pattern, repl
        """
        return self.sub(repl, string, count, pattern, flags)

    def sub2f(self, string, pattern, repl, count=0, flags=0):
        """
        Wrapper for self.sub with string, pattern, formatted repl
        """
        return self.sub(repl, string, count, pattern, flags, True)

    def sub2t(self, string, pattern, repl, count=0, flags=0):
        """
        Wrapper for self.subt with string, pattern, repl
        """
        self.sub(repl, string, count, pattern, flags)
        return self.nsubs

    def sub2ft(self, string, pattern, repl, count=0, flags=0):
        """
        Wrapper for self.subt with string, pattern, formatted repl
        """
        self.sub(repl, string, count, pattern, flags, True)
        return self.nsubs

    def nsubs(self):
        return self.nsubs

    def string(self):
        return self.string

    def groups(self, default=None):
        """
        Returns '' instead of None for no match
        """
        if not self.m: return None
        return map(strnull,self.m.groups(default))

    def group(self, *args):
        if not self.m: return None
        return self.m.group(*args)

    def __getitem__(self, i):
        if not self.m: return None
        return self.m[i]

