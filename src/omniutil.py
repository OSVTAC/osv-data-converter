# -*- coding: utf-8 -*-
#
# Copyright (C) 2020  Carl Hage
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
Utilities for processing omniballot files
"""
import re
from typing import Union, List, Pattern, Match, Dict, IO, AnyStr

def form_bt_suffix(
    s:Dict      # Style object with code and name
    ):
    """
    Converts various conventions for file naming in omniballot to a normalized
    name of the for btnnnps.json where nnn is a 3 digit ballot type, and
    p is an optional party suffix (lower case first letter) and s is an optional
    'n' for no-party voters, e.g. bt001dn.json is ballot type 1 Democratic
    No Party Preference (voters registering as NPP but voting in the Democratic
    presidential primary).
    """
    m = re.match(r'^(?:TYP_BT-)?(\d+)(?: (NP )?([A-Z]+?)(NPP)?)$', s["code"])
    if m:
        bt, pref, party, suff = m.groups()
        party = party[0].lower() if party else ''
        suff = suff[0].lower() if suff else pref[0].lower() if pref else ''
    else:
        party = suff = ''
        m = re.match(r'^(?:Poll BT )?(\d+)$', s["name"])
        if m:
            bt = m.group(1)
        else:
            bt = s["code"]

    bt = bt.zfill(3)
    return  f'{bt}{party}{suff}'

