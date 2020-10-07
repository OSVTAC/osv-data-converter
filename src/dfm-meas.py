#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to convert the CFMJ008 measure list output file:

Input file format:

   Field Name                     Sample
-------------------------------------------------------------
 1  iMeasureID                     1801
 2  sDesignation                   A
 3  szMeasureAbbr1                 Embarcadero Seawall Earthquake Safety Bond
 4  szMeasureAbbr2
 5  iBallotPosition1               1
 6  iBallotPosition2               2
 7  dtDateReceived                 8/15/2018
38  szResponseTitle1               Yes
39  szResponseTitle2               No
40  iSize                          0
44  szGroupHdg                     MEASURES SUBMITTED TO THE VOTERS
45  szBallotHeading                CITY AND COUNTY PROPOSITIONS
46  szSubHeading                   CITY OF SAN FRANCISCO
47  sDistrictID                    *0
48  szDistrictName                 County Wide
49  szElectionDesc                 November 6 2018 Consolidated General Election
50  dtElectionDate                 11/6/2018

"""

import re
import argparse
from zipfile import ZipFile
from tsvio import TSVReader, TSVWriter, DuplicateError

import configEMS

DESCRIPTION = """\
Converts DFM CFMJ008 measure definition file.

Reads the following files:
  * resultdata-raw.zip/CFMJ008.tsv

Creates the following files:
  * measlist-orig.tsv
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
    parser.add_argument('-w', '--warn', action='store_true',
                        help='enable verbose warnings')

    args = parser.parse_args()

    return args

args = parse_args()
config = configEMS.load_ems_config()

separator = config.tsv_separator


headerline = "iMeasureID|sDesignation|szMeasureAbbr1|szMeasureAbbr2|iBallotPosition1"\
    "|iBallotPosition2|dtDateReceived|szReceivedBy|szRemarks|szFilerTitle"\
    "|szFilerName|szFilerFirm|szFilerAddr1|szFilerAddr2|szFilerAddr3"\
    "|szFilerAddr4|szFilerPhone|szFilerFax|szFilerEmail|szContactTitle"\
    "|szContactName|szContactAddr1|szContactAddr2|szContactAddr3"\
    "|szContactAddr4|szContactPhone|szContactFax|szContactEmail"\
    "|szAttorneyTitle|szAttorneyName|szAttorneyAddr1|szAttorneyAddr2"\
    "|szAttorneyAddr3|szAttorneyAddr4|szAttorneyPhone|szAttorneyFax"\
    "|szAttorneyEmail|szResponseTitle1|szResponseTitle2|iSize|sStyle"\
    "|szLegalNoticePubDesc|szDeliveryMethodDesc|szGroupHdg|szBallotHeading"\
    "|szSubHeading|sDistrictID|szDistrictName|szElectionDesc"\
    "|dtElectionDate"

meas_header = "contest_seq|contest_id|district_id|headings|ballot_title|contest_abbr|choice_names"


def joinlist(*args: str, sep:str='~')->str:
    """
    Joins non-null arguments with sep ~
    """
    return sep.join([x for x in args if x])

with ZipFile("ems-raw.zip") as rzip:

    with TSVReader("CFMJ008.tsv", opener=rzip,
                   binary_decode=True, encoding=DFM_ENCODING,
                   validate_header=headerline) as r:
        # Compute a sequence number of original input lines
        seq = 0
        with TSVWriter("measlist-orig.tsv",
                       sort=False,
                       header=meas_header,
                       sep=separator) as w:

            for (iMeasureID, sDesignation, szMeasureAbbr1, szMeasureAbbr2,
                iBallotPosition1, iBallotPosition2, dtDateReceived, szReceivedBy,
                szRemarks, szFilerTitle, szFilerName, szFilerFirm, szFilerAddr1,
                szFilerAddr2, szFilerAddr3, szFilerAddr4, szFilerPhone, szFilerFax,
                szFilerEmail, szContactTitle, szContactName, szContactAddr1,
                szContactAddr2, szContactAddr3, szContactAddr4, szContactPhone,
                szContactFax, szContactEmail, szAttorneyTitle, szAttorneyName,
                szAttorneyAddr1, szAttorneyAddr2, szAttorneyAddr3, szAttorneyAddr4,
                szAttorneyPhone, szAttorneyFax, szAttorneyEmail, szResponseTitle1,
                szResponseTitle2, iSize, sStyle, szLegalNoticePubDesc,
                szDeliveryMethodDesc, szGroupHdg, szBallotHeading, szSubHeading,
                sDistrictID, szDistrictName, szElectionDesc, dtElectionDate
                ) in r.readlines():

                # Trim * prefix on 0 district
                sDistrictID = re.sub(r'^\*','',sDistrictID)

                seq += 1

                w.addline(str(seq).zfill(3), iMeasureID.zfill(config.contest_digits), sDistrictID,
                          joinlist(szGroupHdg, szBallotHeading,szSubHeading),
                          sDesignation,
                          joinlist(szMeasureAbbr1, szMeasureAbbr2),
                          joinlist(szResponseTitle1,szResponseTitle2))
            # End loop over input lines
        # End writing contlist
    # End reading input


