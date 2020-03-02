# Copyright (C) 2020  Chris Jerdonek
#
# This file is part of Open Source Voting Data Converter (ODC).
#
# ODC is free software: you can redistribute it and/or modify
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

"""
Update json/election-base.json
"""

import json
from pathlib import Path
import sys

import utils


LANG_CODE_EN = 'en'

ELECTION_BASE_PATH = Path('json/election-base.json')

TRANSLATIONS_JSON_PATH = Path('submodules/osv-translations/translations.json')


# Dict mapping "result_stat_types" _id to `translations.json` phrase id.
RESULT_STAT_TO_PHRASE_ID = {
    'RSCst': 'category_ballots_cast',
    'RSEli': 'category_eligible_voters',
    'RSExh': 'category_exhausted_ballots',
    'RSOvr': 'category_overvotes',
    'RSReg': 'category_registered_voters',
    'RSRej': 'category_ballots_rejected',
    'RSSki': 'category_skipped_votes',
    'RSTot': 'category_ballots_counted',
    'RSTrn': 'category_voter_turnout',
    'RSUnc': 'category_ballots_uncounted',
    'RSUnd': 'category_undervotes',
    'RSVot': 'category_voters_participating',
    'RSWri': 'category_writein_votes',
}


# Dict mapping "voting_groups" _id to `translations.json` phrase id.
VOTING_GROUP_TO_PHRASE_ID = {
    'ED': 'category_election_day',
    'EV': 'category_early_voting',
    'IA': 'category_in_county',
    'MV': 'category_vote_by_mail',
    'PV': 'category_provisional_voting',
    'TO': 'category_total',
    'XA': 'category_other_counties',
}


def log(text):
    print(text, file=sys.stderr)


def write_json(data, path):
    with open(path, mode='w') as f:
        json.dump(data, f, sort_keys=True, indent=4, ensure_ascii=False)


def update_party(party, phrases_data):
    party_id = party['_id']
    heading = party['heading']

    # This is the key in translations.json.
    phrase_id = f'party_{party_id}'

    try:
        phrase = phrases_data[phrase_id]
    except KeyError:
        log(f'WARNING: phrase {phrase_id!r} missing from: {TRANSLATIONS_JSON_PATH}')
        phrase = {LANG_CODE_EN: heading}

    party['name'] = phrase


def update_parties(base_data, phrases_data):
    parties = base_data['party_names']
    for party in parties:
        update_party(party, phrases_data)


def main():
    base_data = utils.read_json(ELECTION_BASE_PATH)
    translations_data = utils.read_json(TRANSLATIONS_JSON_PATH)
    phrases_data = translations_data['translations']

    update_parties(base_data, phrases_data)

    write_json(base_data, ELECTION_BASE_PATH)


if __name__ == '__main__':
    main()
