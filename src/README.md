# osv-data-converter
Code to support converting election data to and from different formats

##Scripts in the src directory:

* `getomniballot.py` - Fetches the "Accessibe Sample Ballot" from
        omniballot.com with ballot definitions in json format.

* `getsfresults.py` - Fetches summary and detailed results for San
    Francisco (SF/Dominion/WinEDS data formats).

* `getsfems.py` - Fetches DFM-EIMS ems data from SF data downloads

* `checksfsha.py` - Validates the SF SHA512 download with sha512sum.txt

* `convsfresults.py` - Convert the downloaded SF results data

* `convomniballot.py` - Convert the omniballot election definition json. [TODO]
        See comments in the source code for notes on input format.

##Data Field Definitions

* `ballot_type` -
    The official (as printed on ballots) ID for a ballot type, typically
    a number. For elections with party-only voting (party-specific ballots),
    a party suffix is appended to the ballot type (with the suffix
    the field may be called `party_ballot_type`).

* `precinct_id` -
    The official (as printed on voter registration) precinct ID, including
    a split suffix, if applicable. A city/county is subdivided into precincts,
    the smallest area to assign polling locations and report results
    subtotals.

*  `precinct_split_suffix` -
    If the area of a precinct crosses district boundaries, the precinct is
    split into a set sub-areas with a unique combination of districts. An
    ID suffix is assigned to each split, and appended to the base precinct
    ID, with an optional separator character (e.g. `"."`) defined in
    configuration options. If a separator is not used, the configuration
    file can include a pattern to match the base precinct and split suffix
    from the full precinct id.

* `base_precinct_id` -
    The precinct_id without a split suffix.

* `precinct_name` -
    A descriptive name for a precinct. Many jurisdictions do not use
    names for precincts, or are only used internally (as a descriptive
    comment). Instead, the precinct_id is used for public reports.
    An internal-only precinct name might not be unique. (A default

* `cons_precinct_id` -
    The consolidated precinct ID. A group of precincts can be combined
    (consolidated) to a "voting precinct" with the same polling location.
    Precinct subtotals are typically reported at the level of consolidated
    precinct. If precinct consolidation is not defined, the base precinct
    is an implicit consolidation with it's precinct splits.

* `contest_id` -
    The ID that represents a contest (elected office or measure) across
    election data files, typically the ID used in the Election Administration's
    EMS (Election Management System). Different subsystems, e.g. voting
    machines, ballot preparation, vote counting, etc. might have different
    contest IDs. The `ext_contest_ids`

* `rotation_id` -
    An ID representing a set of rules for contest/candidate rotation
    (normally contests order is not rotated). The information defined
    with the rotation ID is typically a rotation_method_id, a randomly
    derived alphabetic mapping, and a district ID. The method ID is
    defined by the rotation software, and includes how an optional
    district

* `district_id` -
    The ID representing a district, a geographic area representing an
    organization (jurisdiction), or a partition or subarea of a organization's
    district.

* `elected_by_district_id` -
    The ID for a district that defines the area of voters who can
    vote on a particular contest. In some cases, the area of voting
    may be different than the area representing an elected office.
    For example, offices "Elected At-Large" are voted on by the
    whole jurisdiction (at-large) while candidates must be residents
    of the district for a seat on a board (e.g. Trustee Area). A
    "County Board of Education", the governing board for a "County
    Office of Education" (COE) may be elected by all voters in a county,
    wheras the area of jurisdiction may cross county boundaries

* `district_name` -
    The name corresponding to a district_id, either the name of an
    organization (jurisdiction), or name of the jurisdiction combined
    with an area portion name (`district_portion`).

* `district_base_name` -
    The portion of a `district_name` representing an organzation/jurisdiction
    as a whole, i.e. the district name without the `district_portion`

* `district_portion` -
    The part of a district_name that represents a partition or subarea
    of a jurisdiction, e.g. Council District, Trustee Area, Division,
    etc. The portion name in data files may have leading spaces in front
    of numbers so a string sort can be used to order numbered districts.
    (One space in front of a single digit, or 2 spaces if the highest
    district is >=100). The portion name might have multiple areas,
    e.g. "District 2 Division 1".

    In some cases, the `district_portion` may represent the area
    defined for another jurisdiction, e.g. "Supervisorial District 1"
    might be used for a party county committee seat with the
    elected-by district corresponding to a County Board of Supervisors
    area.

* `ext_`*name*`_ids` -
    A space separated list of external IDs where *name* matches a defined
    ID field, e.g. `contest` or `district`. This field is used to
    associate the local contest_id, etc. with the corresponding IDs defined
    by other organizations or used within different EMS subsytems. The
    external ID contains a prefix (usually ending a separator character).
    A configuration file contains a table mapping the external ID prefixes
    with an organization/vendor name and URL.

##Source data files:

* `lookups.json` - Precinct to ballot type cross reference
* `bt/btcomposite.json` - Master list of contest definitions
* `bt/bt{{ballot_type}}.json - Contains contests by ballot type
* `odc-config.yaml` - Options to configure the data conversion

##Converted output data files:

* `btpct.tsv` - Ballot type to precinct list

    Fields:
    * `ballot_type` - Official ballot type (typically a number)
    * `precinct_ids` - Space separated list of precinct IDs

* `btcont.tsv` - Ballot type to contest list

    Fields:
    * `ballot_type` - Official ballot type (typically a number with party
        suffix).
    * `contest_ids` - Space separated list of contest IDs. If the election
        contains party-only contests, the ballot type ID may include a party
        suffix, or without the suffix, contains the combined contest IDs.
        Contest IDs are in order of appearance on the ballot. The contest
        ID may be combined with the `":"` separator character and rotation
        ID used to compute candidate order.
