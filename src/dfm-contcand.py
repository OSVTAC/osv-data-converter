#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#

"""
Program to convert the CFMJ001 contest/candidate list output files:

Input file format:

CFMJ001_ContestData.tsv 
    Field Name                     Sample
-------------------------------------------------------------
 1  ELECTION                       March 3 2020 Consolidated Presidential Primary Election
 2  CONTESTID                      100
 3  CONTESTTITLE1                  PRESIDENT DEM
 4  CONTESTTITLE2                  District 12
 5  NUMTOVOTFOR                    1
 6  TERMOFOFFICE                   4
 7  FILINGFEE                      0
 8  NUMOFCANDS                     20
 9  NUMQUALIFIEDCANDS              20
10  NUMTOVOTFOR                    1
11  DECOFINTENT                    0
12  FILINGEXTENSION                0
13  CANDSTMT                       0
14  DISTRICTID                     *0
15  SUBDISTRICT                    0
16  CONTESTPARTY                   DEM

CFMJ001_ContestCandidateData.tsv 
   Field Name                     Sample
-------------------------------------------------------------
 1  iContestID                     100
 2  szBallotHeading                FEDERAL
 3  szSubHeading                   
 4  szOfficeTitle                  PRESIDENT OF THE UNITED STATES-DEM
 5  szOfficeAbbr1                  PRESIDENT DEM
 6  szOfficeAbbr2                  DEM
 7  sGoesIntoExtension             N
 8  iNumToVoteFor                  1
 9  iOfficeOnBallot                1
10  iNumQualified                  20
11  iNumCandidates                 20
12  iCandidateID                   6
13  szCandidateName                MICHAEL BENNET
14  szBallotDesignation            Member of Congress
15  iIncumbent                     0
16  dtQualified                    12/11/2019 12:48:00 PM
17  dtSigsInLieuFiled_dt           11/6/2019
18  dtSigsInLieuIssued_dt          
19  dtDecOfIntentIssued_dt         
20  dtDecOfIntentFiled_dt          
21  dtNOMIssued_dt                 11/14/2019
22  dtNomFiled_dt                  12/6/2019
23  dtFilingFeePaid_dt             11/14/2019
24  dtCandStmtFiled_dt             12/6/2019
25  dtCandStmtIssued_dt            11/14/2019
26  dtDecOfCandIssued_dt           11/21/2019
27  dtDecOfCandFiled_dt            12/5/2019
28  dtCodeFairCampFiled_dt         
29  sUserCode1                     F
30  sUserCode2                     
31  szResAddress1                  
32  szResAddress2                  
33  szResAddress3                  
34  szMailAddr1                    
35  szMailAddr2                    
36  szMailAddr3                    
37  szMailAddr4                    
38  szBusinessAddr1                
39  szBusinessCity                 
40  szBusinessState                
41  szBusinessZip                  
42  sBusinessPhone                 
43  sHomePhone                     
44  sFaxNo                         
45  szEmailAddress                 
46  sPartyAbbr                     
47  szPartyName                    
48  sCampaignPhone                 
49  sCampaignFax                   
50  sCampaignMobile                
51  szWebAddress                   
52  sElectronicCandStmt            No

"""

import re
import argparse
from zipfile import ZipFile
from tsvio import TSVReader, TSVWriter, DuplicateError

import configEMS

DESCRIPTION = """\
Converts DFM CFMJ001 contest and candidate definition files.

Reads the following files:
  * ems-raw.zip/CFMJ001_ContestData.tsv
  * ems-raw.zip/CFMJ001_ContestCandidateData.tsv

Creates the following files:
  * contlist-orig.tsv
  * candlist-orig.tsv
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
    parser.add_argument('-p', dest='pipe', action='store_true',
                        help='use pipe separator else tab')

    args = parser.parse_args()

    return args

args = parse_args()
config = configEMS.load_ems_config()

# The CFMJ001_ContestData file contains basic contest info
contheader = "ELECTION|CONTESTID|CONTESTTITLE1|CONTESTTITLE2|NUMTOVOTFOR"\
    "|TERMOFOFFICE|FILINGFEE|NUMOFCANDS|NUMQUALIFIEDCANDS|NUMTOVOTFOR"\
    "|DECOFINTENT|FILINGEXTENSION|CANDSTMT|DISTRICTID|SUBDISTRICT"\
    "|CONTESTPARTY"

# The
candheader = "iContestID|szBallotHeading|szSubHeading|szOfficeTitle|szOfficeAbbr1"\
    "|szOfficeAbbr2|sGoesIntoExtension|iNumToVoteFor|iOfficeOnBallot"\
    "|iNumQualified|iNumCandidates|iCandidateID|szCandidateName"\
    "|szBallotDesignation|iIncumbent|dtQualified|dtSigsInLieuFiled_dt"\
    "|dtSigsInLieuIssued_dt|dtDecOfIntentIssued_dt|dtDecOfIntentFiled_dt"\
    "|dtNOMIssued_dt|dtNomFiled_dt|dtFilingFeePaid_dt|dtCandStmtFiled_dt"\
    "|dtCandStmtIssued_dt|dtDecOfCandIssued_dt|dtDecOfCandFiled_dt"\
    "|dtCodeFairCampFiled_dt|sUserCode1|sUserCode2|szResAddress1"\
    "|szResAddress2|szResAddress3|szMailAddr1|szMailAddr2|szMailAddr3"\
    "|szMailAddr4|sPhone|sAltPhone|sFaxNo|szEmailAddress|sPartyAbbr"\
    "|szPartyName|sCampaignPhone|sCampaignFax|sCampaignMobile|szWebAddress"\
    "|sElectronicCandStmt"

candheader2 = "iContestID|szBallotHeading|szSubHeading|szOfficeTitle|szOfficeAbbr1"\
    "|szOfficeAbbr2|sGoesIntoExtension|iNumToVoteFor|iOfficeOnBallot"\
    "|iNumQualified|iNumCandidates|iCandidateID|szCandidateName"\
    "|szBallotDesignation|iIncumbent|dtQualified|dtSigsInLieuFiled_dt"\
    "|dtSigsInLieuIssued_dt|dtDecOfIntentIssued_dt|dtDecOfIntentFiled_dt"\
    "|dtNOMIssued_dt|dtNomFiled_dt|dtFilingFeePaid_dt|dtCandStmtFiled_dt"\
    "|dtCandStmtIssued_dt|dtDecOfCandIssued_dt|dtDecOfCandFiled_dt"\
    "|dtCodeFairCampFiled_dt|sUserCode1|sUserCode2|szResAddress1"\
    "|szResAddress2|szResAddress3|szMailAddr1|szMailAddr2|szMailAddr3"\
    "|szMailAddr4|szBusinessAddr1|szBusinessCity|szBusinessState"\
    "|szBusinessZip|sBusinessPhone|sHomePhone|sFaxNo|szEmailAddress"\
    "|sPartyAbbr|szPartyName|sCampaignPhone|sCampaignFax|sCampaignMobile"\
    "|szWebAddress|sElectronicCandStmt"

candlist_header = "contest_id|cand_id|cand_name|ballot_designation|cand_party|incumbent|qualified"
contlist_header = "contest_seq|contest_id|district_id|headings|ballot_title|contest_abbr|contest_party|vote_for|on_ballot"

separator = config.tsv_separator

cont_extra = {} # Extra contest definitions in candidate file

def boolstr(x) -> str:
    """
    Converts a boolean value to Y/N or blank for None
    """
    return '' if x == None else 'Y' if x=='1' or x=='Y' or x==True else 'N'

with ZipFile("ems-raw.zip") as rzip:

    with TSVReader("CFMJ001_ContestCandidateData.tsv", opener=rzip,
                   binary_decode=True, encoding=DFM_ENCODING) as r:

        if r.headerline == candheader: candformat = 1
        elif r.headerline == candheader2: candformat = 2
        else: raise RuntimeError(f"Mismatched header in CFMJ001_ContestCandidateData:\n   {r.headerline}\n!= {candheader2}")

        with TSVWriter("candlist-orig.tsv", False, separator, candlist_header) as w:

            for cols in r.readlines():

                if candformat==2:
                    (iContestID, szBallotHeading, szSubHeading, szOfficeTitle,
                    szOfficeAbbr1, szOfficeAbbr2, sGoesIntoExtension, iNumToVoteFor,
                    iOfficeOnBallot, iNumQualified, iNumCandidates, iCandidateID,
                    szCandidateName, szBallotDesignation, iIncumbent, dtQualified,
                    dtSigsInLieuFiled_dt, dtSigsInLieuIssued_dt, dtDecOfIntentIssued_dt,
                    dtDecOfIntentFiled_dt, dtNOMIssued_dt, dtNomFiled_dt,
                    dtFilingFeePaid_dt, dtCandStmtFiled_dt, dtCandStmtIssued_dt,
                    dtDecOfCandIssued_dt, dtDecOfCandFiled_dt, dtCodeFairCampFiled_dt,
                    sUserCode1, sUserCode2, szResAddress1, szResAddress2, szResAddress3,
                    szMailAddr1, szMailAddr2, szMailAddr3, szMailAddr4, szBusinessAddr1,
                    szBusinessCity, szBusinessState, szBusinessZip, sBusinessPhone,
                    sHomePhone, sFaxNo, szEmailAddress, sPartyAbbr, szPartyName,
                    sCampaignPhone, sCampaignFax, sCampaignMobile, szWebAddress,
                    sElectronicCandStmt) = cols
                else:
                    (iContestID, szBallotHeading, szSubHeading, szOfficeTitle,
                    szOfficeAbbr1, szOfficeAbbr2, sGoesIntoExtension, iNumToVoteFor,
                    iOfficeOnBallot, iNumQualified, iNumCandidates, iCandidateID,
                    szCandidateName, szBallotDesignation, iIncumbent, dtQualified,
                    dtSigsInLieuFiled_dt, dtSigsInLieuIssued_dt, dtDecOfIntentIssued_dt,
                    dtDecOfIntentFiled_dt, dtNOMIssued_dt, dtNomFiled_dt,
                    dtFilingFeePaid_dt, dtCandStmtFiled_dt, dtCandStmtIssued_dt,
                    dtDecOfCandIssued_dt, dtDecOfCandFiled_dt, dtCodeFairCampFiled_dt,
                    sUserCode1, sUserCode2, szResAddress1, szResAddress2, szResAddress3,
                    szMailAddr1, szMailAddr2, szMailAddr3, szMailAddr4, sPhone, sAltPhone,
                    sFaxNo, szEmailAddress, sPartyAbbr, szPartyName, sCampaignPhone,
                    sCampaignFax, sCampaignMobile, szWebAddress, sElectronicCandStmt
                    ) = cols
                # The CFMJ001 is brain-damaged-- the CFMJ001_ContestCandidateData
                # has missing contest definition data, and can be lost if there
                # are no candidates. The on/off ballot indicator may be wrong if there
                # are only write-in slots.

                # Save the contest information for later
                contline = w.joinline(szBallotHeading, szSubHeading, szOfficeTitle,
                                    szOfficeAbbr1, szOfficeAbbr2, sGoesIntoExtension, iNumToVoteFor,
                                    iOfficeOnBallot, iNumQualified, iNumCandidates)
                if iContestID in cont_extra:
                    if contline != cont_extra[iContestID]:
                        raise DuplicateError(
            f"Mismatched contest info:\n   {cont_extra[iContestID]}!= {contline}")
                else:
                    cont_extra[iContestID] = contline

                # DFM candidate IDs are a sequence 1..n
                # For now don't append the contest ID
                iCandidateID = iCandidateID.zfill(3)

                # Contact info not extracted now [TODO]
                szCandidateName = szCandidateName.rstrip('*')
                w.addline(iContestID.zfill(config.contest_digits),
                          iCandidateID, szCandidateName,
                          szBallotDesignation, szPartyName, boolstr(iIncumbent),
                          boolstr(dtQualified != ""))
            # End loop over input lines
        # End writing output file
    # End reading input file

    with TSVReader("CFMJ001_ContestData.tsv", opener=rzip,
                   binary_decode=True, encoding=DFM_ENCODING,
                   validate_header=contheader) as r:
        seq = 0
        with TSVWriter("contlist-orig.tsv", True, separator, contlist_header) as w:
            for (election, contestid, contesttitle1, contesttitle2, numtovotfor,
                termofoffice, filingfee, numofcands, numqualifiedcands, numtovotfor1,
                decofintent, filingextension, candstmt, districtid, subdistrict,
                contestparty
                ) in r.readlines():

                if contesttitle2: # Append second line of contest abbr with ~
                    contesttitle1 += "~" + contesttitle2

                if subdistrict and subdistrict != '0':
                    # trustee areas etc are represented with -nn suffix
                    districtid += "-" + subdistrict.zfill(2)

                # Trim * prefix on 0
                districtid = re.sub(r'^\*','',districtid)

                # Look for extra information
                if contestid in cont_extra:
                    # Get saved info
                    (szBallotHeading, szSubHeading, szOfficeTitle,
                    szOfficeAbbr1, szOfficeAbbr2, sGoesIntoExtension, iNumToVoteFor,
                    iOfficeOnBallot, iNumQualified, iNumCandidates
                    ) = cont_extra[contestid].split(separator)

                    # Concatenate multiple lines
                    if szSubHeading: # Append subheading with ~
                        szBallotHeading += "~" + szSubHeading
                    if szOfficeAbbr2:
                        szOfficeAbbr1 += "~" + szOfficeAbbr2

                    if szOfficeAbbr1 != contesttitle1 and args.warn:
                        print(f"Strange contest abbr {contestid} '{szOfficeAbbr1}' != '{contesttitle1}'")
                else:
                    iOfficeOnBallot = ''
                    szBallotHeading = szOfficeAbbr1 = ''
                    szOfficeTitle = contesttitle1
                    iNumToVoteFor = numtovotfor

                if numtovotfor != numtovotfor1:
                    print(f"Strange vote for number in {contestid}:{szOfficeAbbr1} {numtovotfor} != {numtovotfor1}")

                seq += 1

                w.addline(str(seq).zfill(3), 
                          contestid.zfill(config.contest_digits), districtid,
                          szBallotHeading, szOfficeTitle, contesttitle1,
                          contestparty, numtovotfor, boolstr(iOfficeOnBallot))
            # End loop over input lines
        # End writing contlist
    # End reading input


