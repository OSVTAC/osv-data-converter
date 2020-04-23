#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to extract precinct and poll information in
ems-raw.zip/PODJ011.tsv

"""

import argparse
from zipfile import ZipFile
from tsvio import TSVReader, TSVWriter

import configEMS

DESCRIPTION = """\
Converts DFM PODJ011.tsv list of poll locations with precinct
consolidation and ballot type.

Reads the following files:
  * emsdata-raw.zip/PODJ011.tsv

Creates the following files:
  * precinct.tsv - precinct with consolidation, ballot type, VBM, and poll ID
  * pollplace-orig.tsv - poll locations, raw output
"""
VERSION='0.0.1'     # Program version

DFM_ENCODING = 'ISO-8859-1'
OUT_ENCODING = 'UTF-8'

def parse_args():
    """
    Parse sys.argv and return a Namespace object.
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION,
                    formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='enable verbose info printout')
    parser.add_argument('-t', '--traceinp', action='store_true',
                        help='print input found')

    args = parser.parse_args()

    return args

args = parse_args()
config = configEMS.load_ems_config()

separator = config.tsv_separator

podj011_headerline = "ElectionAbbr|PollingSiteID|PollingSiteName|TableID|PollingPlaceDesc1"\
    "|PollingPlaceDesc2|SamBalAddr1|SamBalAddr2|VotingPctID|VotingPctName"\
    "|PollPlaceRegPctID|PollPlaceRegPctPortion"\
    "|BallotTypeList"

with TSVWriter("precinct.tsv",
                header="precinct_id|cons_precinct_id|cons_precinct_name|ballot_type|vbm|poll_id",
                sep=separator,
                unique_col_check=0) as pctfile:
  with TSVWriter("pollplace-orig.tsv",
                header="poll_id|poll_location|poll_directions|poll_address|poll_city|accessibility",
                sep=separator,
                unique_col_check=0) as pollfile:

    with ZipFile("ems-raw.zip") as rzip:
        with TSVReader("PODJ011.tsv", opener=rzip, strip_spaces=True,
                    binary_decode=True, encoding=DFM_ENCODING,
                    validate_header=podj011_headerline) as r:

            # Get the column index from the header name
            header_index = {v.replace(" ","_"):i for i,v in enumerate(r.header) }

            for cols in r.readlines():

                (ElectionAbbr, PollingSiteID, PollingSiteName, TableID,
                 PollingPlaceDesc1, PollingPlaceDesc2, SamBalAddr1, SamBalAddr2,
                 VotingPctID, VotingPctName, PollPlaceRegPctID, PollPlaceRegPctPortion,
                 BallotTypeList) = cols

                if args.traceinp:
                     print('|'.join(cols))

                if TableID:
                    PollingSiteID += '.'+TableID

                def get_named_cols(col_names:str)->str:
                    """
                    Retrieves the value of the named column. If a space
                    separated list of names is given, return a newline \n
                    seprated list of values.
                    """
                    if not col_names:
                        return ""

                    return "\n".join([ cols[header_index[n]]
                                      for n in col_names.split() ])

                    

                # If the ballot type has more than one value, we need to compute it.
                # Bug in DFM: the ballot type is combined for the voting precinct,
                # not the regular precinct
                vbm = ('' if not config.poll_vote_by_mail_pattern else
                       'Y' if config.poll_vote_by_mail_pattern.search(
                               get_named_cols(config.poll_vote_by_mail_column)) else
                       'N')
                
                if vbm == 'Y':
                    PollingSiteID = 'Mail'
                else:
                    pollfile.addline(PollingSiteID,
                                 get_named_cols(config.poll_location_columns),
                                 get_named_cols(config.poll_directions_columns),
                                 get_named_cols(config.poll_address_columns),
                                 get_named_cols(config.poll_city_columns),
                                 get_named_cols(config.poll_accessibility_columns),)
            
                pctfile.addline(PollPlaceRegPctID+PollPlaceRegPctPortion,
                                VotingPctID, VotingPctName,
                                BallotTypeList.zfill(config.bt_digits), vbm, PollingSiteID)


