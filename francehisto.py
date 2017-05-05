import csv
import os

# Initial downloads.
BASE = 'https://github.com/etalab/geohisto/'
URLS = [
    BASE + 'raw/master/exports/towns/towns.csv',
    BASE + 'raw/master/exports/counties/counties.csv',
    BASE + 'raw/master/exports/regions/regions.csv'
]


def _iter_over_csv(filename):
    """Generator to iterate of a CSV file, return a tuple (index, content)."""
    with open(filename) as csv_file:
        for i, line in enumerate(csv.DictReader(csv_file)):
            yield i, line


def load_communes(zones, root):
    """Load towns from GeoHisto."""
    filename = os.path.join(root, 'towns.csv')
    data = [{
        '_id': line['id'],
        'code': line['insee_code'],
        'level': 'fr/commune',
        'name': line['name'],
        'population': (0
                       if line['population'] == 'NULL'
                       else int(line['population'])),
        'parents': (['country/fr', 'country-group/ue', 'country-group/world'] +
                    line['parents'].split(';')),
        'keys': {
            'insee': line['insee_code'],
        },
        'successors': line['successors'].split(';'),
        'ancestors': line['ancestors'].split(';'),
        'validity': {
            'start': line['start_datetime'].split(' ')[0],
            'end': line['end_datetime'].split(' ')[0]
        }
    } for i, line in _iter_over_csv(filename)]
    result = zones.insert_many(data)
    return len(result.inserted_ids)


def load_departements(zones, root):
    """Load departements from GeoHisto."""
    filename = os.path.join(root, 'counties.csv')
    data = [{
        '_id': line['id'],
        'code': line['insee_code'],
        'level': 'fr/departement',
        'name': line['name'],
        'parents': (['country/fr', 'country-group/ue', 'country-group/world'] +
                    line['parents'].split(';')),
        'keys': {
            'insee': line['insee_code'],
        },
        'successors': line['successors'].split(';'),
        'ancestors': line['ancestors'].split(';'),
        'validity': {
            'start': line['start_datetime'].split(' ')[0],
            'end': line['end_datetime'].split(' ')[0]
        }
    } for i, line in _iter_over_csv(filename)]
    result = zones.insert_many(data)
    return len(result.inserted_ids)


def load_regions(zones, root):
    """Load regions from GeoHisto."""
    filename = os.path.join(root, 'regions.csv')
    data = [{
        '_id': line['id'],
        'code': line['insee_code'],
        'level': 'fr/region',
        'name': line['name'],
        'area': int(line['surface']),
        'population': int(line['population']),
        'wikipedia': line['wikipedia'],
        'parents': [
            'country/fr', 'country-group/ue', 'country-group/world'
        ],
        'keys': {
            'insee': line['insee_code'],
            'nuts2': line['nuts_code']
        },
        'successors': line['successors'].split(';'),
        'ancestors': line['ancestors'].split(';'),
        'validity': {
            'start': line['start_datetime'].split(' ')[0],
            'end': line['end_datetime'].split(' ')[0]
        }
    } for i, line in _iter_over_csv(filename)]
    result = zones.insert_many(data)
    return len(result.inserted_ids)


def retrieve_zones(db, level, code=None, before=None, after=None):
    """
    Retrieve zones for a given level and code before/after a date
    including that date.

    The `before` and `after` dates must be strings like `YYYY-MM-DD`.
    They are mutually exclusive.
    """
    if before:
        end = {'$lte': before}
    elif after:
        end = {'$gte': after}
    else:
        raise ValueError('You must set the "before" or "after" parameters.')
    conditions = {
        'level': level,
        'validity.end': end,
    }
    if code:
        conditions['code'] = code
    return db.find(conditions).sort('-validity.start')


def retrieve_zone(db, level, code=None, before=None, after=None):
    """
    Retrieve the latest zone for a given level and code before/after a date
    including that date.

    The `before` and `after` dates must be a strings like `YYYY-MM-DD`.
    They are mutually exclusive.
    """
    zone = list(retrieve_zones(db, level, code, before, after).limit(1))
    return zone and zone[0] or None


def retrieve_current_departements(db):
    return retrieve_zones(db, 'fr/departement', after='2016-01-01')


def retrieve_current_metro_departements(db):
    return [zone for zone in retrieve_current_departements(db)
            if len(zone['code']) == 2]


def retrieve_current_drom_departements(db):
    return [zone for zone in retrieve_current_departements(db)
            if len(zone['code']) == 3]


def retrieve_current_departement(db, code=None):
    return retrieve_zone(db, 'fr/departement', code, after='2016-01-01')


def retrieve_current_region(db, code=None):
    return retrieve_zone(db, 'fr/region', code, after='2016-01-01')
