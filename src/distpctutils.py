# -*- coding: utf-8 -*-
#

"""
Utilities for processing district names
"""

import re
from zipfile import ZipFile
from tsvio import TSVReader, TSVWriter
from typing import (Dict, Tuple, List, Set, TextIO, Union, Callable,
                     NamedTuple, Iterable, Set, Any, Optional)
from collections import defaultdict

distclass_header = "District_Code|Classification|District_Name"\
    "|District_Short_Name"

distextra_header = "District_Code|District_Name|Portion_Codes"

distcont_header = "district_id|district_name|contest_ids"

precinct_header = "precinct_id|cons_precinct_id|cons_precinct_name"\
    "|ballot_type|vbm|poll_id"

def alphanumeric_sort_key(key:str)->str:
    """
    Returns a sort key with numbers expanded to 3 digits
    """

    return re.sub(r'\d+',lambda m: m.group(0).zfill(3), key)

#--------------------------
# Routines to load district definition data

def load_distnames(filename="distclass.tsv"):
    """
    Load the distclass.tsv to get a dictionary of
    district names by ID code.
    """
    with TSVReader(filename, validate_header=distclass_header) as r:
        return r.load_simple_dict('District_Code', 'District_Name')

def load_distclass(filename="distclass.tsv"):
    """
    Load the distclass.tsv to get a dictionary of
    named tuples by ID code.
    """
    with TSVReader(filename, validate_header=distclass_header) as r:
        return r.load_tuple_dict('DistrictClassification')

def load_distcont(filename="edistcont.tsv"):
    """
    Load the distcont.tsv to get a dictionary of
    named tuples by ID code for districts in the election.
    """
    with TSVReader(filename, validate_header=distcont_header) as r:
        return r.load_tuple_dict('DistrictContests')



#--------------------------
# Routines to process distpct data
#--------------------------

def enter_distextra(
        distpct:Dict[str,Set[str]],         # district to precincts dictionary
        distname:Dict[str,str]={},          # district name table
        filename="distextra.tsv",           # file to read
        )->Dict[str,str]:                   # returns distname
    """
    Reads the distextra.tsv and creates additional districts, from the 
    base definitions in distpct. The Portion_Codes is a space separated
    list of other districts that compose
    """
    with TSVReader(filename, validate_header=distextra_header) as r:
        for (District_Code, District_Name, Portion_Codes) in r.readlines():
            portions = Portion_Codes.split()
            distname[District_Code] = District_Name
            if len(portions)==1 and not Portion_Codes.endswith('?'):
                distpct[District_Code] = distpct[Portion_Codes]
                continue

            precincts = set()
            for portion in portions:
                optional = portion.endswith("?")
                if optional:
                    portion = portion[:-1]
                    precincts.update([ p if p.endswith('?') else p+'?'
                                     for p in distpct[portion] ])
                else:
                    precincts.update(distpct[portion])
            distpct[District_Code] = precincts
            

    return distname

#--------------------------

def split_check(ids:Union[str,Iterable[str]])->Iterable[str]:
    """
    Utility routine to split a string into a list if needed
    """
    if isinstance(ids, str):
        ids = ids.split()
    return ids

def trim_extra_possible(id_set:Set[str]):
    """
    For an ID set that may have IDs with ?, remove the
    ID? if ID is defined.
    """
    for i in id_set:
        if i.endswith('?') and i[:-1] in id_set:
            id_set.remove(i)

#--------------------------

def distpct_inverse(
        distpct:Dict[str,Union[str,Iterable[str]]], # district ID to precinct ID list
        select:Set[str]={},                 # Optional filter to select districts
        )->Dict[str,Set[str]]:              # precinct ID to district ID list
    """
    Computes the inverse of a district->precinct or
    precinct->district map. The precinct list can be an unsplit string
    or a list/set. If the filter is supplied, then extract only the selected
    districts.
    """
    pctdist = defaultdict(lambda:set())
    for district_id, precinct_ids in sorted(split_check(distpct).items()):
        if select and district_id not in select:
            continue
        for pct in split_check(precinct_ids):
            pctdist[pct].add(district_id)

    return pctdist

#--------------------------
# Form 

def form_distpct_set(
        distpct:Dict[str,Iterable[str]], # distpct or pctdist
        keysort:Optional[Callable]=None, # for sorting IDs
        )->Dict[str,List[str]]:
    """
    Returns a dictionary of precinct sets with a list of
    district_ids for the distpct dictionary or vice versa.
    """
    # Reform with a precinct set
    pctset = defaultdict(lambda:list())
    for district_id, precinct_ids in distpct.items():
        trim_extra_possible(precinct_ids)
        precinct_ids = ' '.join(sorted(precinct_ids, key=keysort))
        pctset[precinct_ids].append(district_id)
    return pctset
#--------------------------
# Routines to read and write distpct files

def write_distpct(
        distpct:Dict[str,Iterable[str]],    # district to precincts dictionary
        filename="distpct.tsv.gz",          # file to write
        sep='\t',                           # column delimiter
        keysort:Optional[Callable]=None,    # for sorting IDs
        ):
    """
    Writes the distpct data to a file, combining precincts
    """
    # Reform with a precinct set
    pctset = form_distpct_set(distpct, keysort)

    with TSVWriter(filename,sep=sep,sort=True,
                    header="district_ids|precinct_set") as w:
        for precinct_ids, district_ids  in pctset.items():
            w.addline(' '.join(district_ids), precinct_ids)

#--------------------------

def write_pctdist(
        pctdist:Dict[str,List[str]],        # district to precincts dictionary
        filename="pctdist.tsv.gz",          # file to write
        sep='\t',                           # column delimiter
        keysort:Optional[Callable]=None,    # for sorting IDs
        ):
    """
    Writes the inverse pctdist data to a file, combining precincts
    """
    # Compute district sets
    distset = form_distpct_set(pctdist, keysort)

    with TSVWriter(filename,sep=sep,sort=True,
                    header="precinct_ids|district_set") as w:
        for district_ids, precinct_ids,  in distset.items():
            w.addline(' '.join(precinct_ids), district_ids)


distpct_headers = { "precinct_ids|district_set", "district_ids|precinct_set"}

#--------------------------

def load_distpct(
        filename="distpct.tsv.gz",          # file to read
        select:Dict[str,Any]={},            # Optional filter to select districts
        splitset:bool=False                 # True to split the precinct_set
        )->Dict[str,Union[str,List[str]]]:  # Returns the unsplit/split set
    """
    Loads a distpct or pctdist file, returning the unsplit pct/dist set.
    If select is provided, only the selected districts/precincts are included.
    """
    distpct = {}
    with TSVReader(filename,validate_header=distpct_headers) as r:
        for district_ids, precinct_set in r.readlines():
            for dist in district_ids.split():
                if select and dist not in select:
                    continue
                if splitset:
                    precinct_set = precinct_set
                distpct[dist] = precinct_set
    
    return distpct

#--------------------------
# Routines to load precinct consolidation definition data

def load_precinct(filename="precinct.tsv"):
    """
    Load the distprecinct.tsv to get a dictionary of
    named tuples by ID code.
    """
    with TSVReader(filename, validate_header=precinct_header) as r:
        return r.load_tuple_dict('PrecinctBT')

#--------------------------
# Routines for party registration and turnout

def load_cpctreg(filename="cpctreg.tsv"):
    """
    Load the consolidated precinct registration into a
    namedtuple by header.
    """
    with TSVReader(filename) as r:
        return r.load_tuple_dict('CPctReg')




