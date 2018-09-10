import csv
import os

# Initial downloads.
BASE = 'https://github.com/etalab/geohisto/raw/master/'
URLS = [
    BASE + 'exports/communes/communes.csv',
    BASE + 'exports/departements/departements.csv',
    BASE + 'exports/collectivites/collectivites.csv',
    BASE + 'exports/regions/regions.csv',
    BASE + 'exports/epci/epci.csv',
]


def _iter_over_csv(filename):
    """Generator to iterate over the lines of a CSV file as dict"""
    with open(filename) as csv_file:
        for line in csv.DictReader(csv_file):
            yield line


def load_communes(zones, root):
    """Load towns from GeoHisto."""
    filename = os.path.join(root, 'communes.csv')
    data = [{
        '_id': line['id'],
        'code': line['insee_code'],
        'level': 'fr:commune',
        'name': line['name'],
        'population': (0
                       if line['population'] == 'NULL'
                       else int(line['population'])),
        'parents': (['country:fr', 'country-group:ue', 'country-group:world'] +
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
    } for line in _iter_over_csv(filename)]
    return zones.safe_bulk_insert(data)


def load_departements(zones, root):
    """Load departements from GeoHisto."""
    filename = os.path.join(root, 'departements.csv')
    data = [{
        '_id': line['id'],
        'code': line['insee_code'],
        'level': 'fr:departement',
        'name': line['name'],
        'parents': (['country:fr', 'country-group:ue', 'country-group:world'] +
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
    } for line in _iter_over_csv(filename)]
    return zones.safe_bulk_insert(data)


def load_collectivites(zones, root):
    """Load collectivites from GeoHisto."""
    filename = os.path.join(root, 'collectivites.csv')
    data = [{
        '_id': line['id'],
        'code': line['insee_code'],
        'level': 'fr:collectivite-outre-mer',
        'name': line['name'],
        'iso2': line['iso2'],
        'parents': (['country:fr', 'country-group:ue', 'country-group:world'] +
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
    } for line in _iter_over_csv(filename)]
    return zones.safe_bulk_insert(data)


def load_regions(zones, root):
    """Load regions from GeoHisto."""
    filename = os.path.join(root, 'regions.csv')
    data = [{
        '_id': line['id'],
        'code': line['insee_code'],
        'level': 'fr:region',
        'name': line['name'],
        'area': int(line['surface']),
        'population': int(line['population']),
        'wikipedia': line['wikipedia'],
        'parents': [
            'country:fr', 'country-group:ue', 'country-group:world'
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
    } for line in _iter_over_csv(filename)]
    return zones.safe_bulk_insert(data)


def load_epcis(zones, root):
    """Load EPCIs from GeoHisto."""
    filename = os.path.join(root, 'epci.csv')
    data = [{
        '_id': line['id'],
        'code': line['siren'],
        'level': 'fr:epci',
        'name': line['name'],
        'population': int(line['population']),
        'parents': [
            'country:fr', 'country-group:ue', 'country-group:world'
        ],
        'keys': {
            'siren': line['siren'],
            'type_epci': line['kind'],
        },
        'successors': line['successors'].split(';'),
        'ancestors': line['ancestors'].split(';'),
        '_towns': line['towns'].split(';'),
        'validity': {
            'start': line['start_date'],
            'end': line['end_date']
        }
    } for line in _iter_over_csv(filename)]
    return zones.safe_bulk_insert(data)


def retrieve_current_departements(db):
    return db.fetch_zones('fr:departement', after='2016-01-01')


def retrieve_current_metro_departements(db):
    return [zone for zone in retrieve_current_departements(db)
            if len(zone['code']) == 2]


def retrieve_current_drom_departements(db):
    return [zone for zone in retrieve_current_departements(db)
            if len(zone['code']) == 3]


def retrieve_current_departement(db, code=None):
    return db.fetch_zone('fr:departement', code, after='2016-01-01')


def retrieve_current_collectivites(db):
    return db.fetch_zones('fr:collectivite-outre-mer', after='2016-01-01')


def retrieve_current_collectivite(db, iso2):
    zone = list(db.find({
        'level': 'fr:collectivite-outre-mer', 'iso2': iso2
    }).limit(1))
    return zone and zone[0] or None


def retrieve_current_region(db, code=None):
    return db.fetch_zone('fr:region', code, after='2016-01-01')
