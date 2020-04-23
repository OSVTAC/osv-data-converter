# -*- coding: utf-8 -*-
#

"""
Utilities for processing district names
"""

from zipfile import ZipFile
from tsvio import TSVReader, TSVWriter
from typing import Dict, Tuple, List, Set, TextIO, Union, NamedTuple, Iterable, Set
from collections import defaultdict

distclass_header = "District_Code|Classification|District_Name"\
    "|District_Short_Name"

distextra_header = "District_Code|District_Name|Portion_Codes"

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
        for pct in precinct_ids:
            pctdist[pct].add(district_id)

    return pctdist

#--------------------------
# Routines to read and write distpct files

def write_distpct(
        distpct:Dict[str,Iterable[str]],        # district to precincts dictionary
        filename="distpct.tsv.gz",          # file to write
        sep='\t'                            # column delimiter
        ):
    """
    Writes the distpct data to a file, combining precincts
    """
    # Reform with a precinct set
    pctset = defaultdict(lambda:list())
    for district_id, precinct_ids in sorted(distpct.items()):
        trim_extra_possible(precinct_ids)
        precinct_ids = ' '.join(sorted(precinct_ids))
        pctset[precinct_ids].append(district_id)


    with TSVWriter("distpct.tsv.gz",sep=sep,sort=True,
                    header="district_ids|precinct_set") as w:
        for precinct_ids, district_ids  in pctset.items():
            w.addline(' '.join(district_ids), precinct_ids)

#--------------------------

def write_pctdist(
        distpct:Dict[str,List[str]],        # district to precincts dictionary
        filename="pctdist.tsv.gz",          # file to write
        sep='\t'                            # column delimiter
        ):
    """
    Writes the inverse pctdist data to a file, combining precincts
    """
    # Compute the inverse of precinct to district
    pctdist = distpct_inverse(distpct)

    # Compute district sets
    distset = defaultdict(lambda:list())
    for precinct_id, district_ids,  in sorted(pctdist.items()):
        trim_extra_possible(district_ids)
        district_ids = ' '.join(sorted(district_ids))
        distset[district_ids].append(precinct_id)

    with TSVWriter("pctdist.tsv.gz",sep=sep,sort=True,
                    header="precinct_ids|district_set") as w:
        for district_ids, precinct_ids,  in distset.items():
            w.addline(' '.join(precinct_ids), district_ids)


distpct_headers = { "precinct_ids|district_set", "district_ids|precinct_set"}

#--------------------------

def load_distpct(
        filename="distpct.tsv.gz",          # file to read
        select:Set[str]={},                 # Optional filter to select districts
        )->Dict[str,str]:                   # Returns the unsplit set
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
                distpct[dist] = precinct_set
    
    return distpct

