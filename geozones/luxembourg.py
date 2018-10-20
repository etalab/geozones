from .model import Level, country

_ = lambda s: s  # noqa: E731

district = Level('lu:district', _('Luxembourguish district'), 40, country)
canton = Level('lu:canton', _('Luxembourguish canton'), 60, district)
commune = Level('lu:commune', _('Luxembourguish commune'), 80, canton)

# See: https://data.public.lu/fr/datasets/limites-administratives-du-grand-duche-de-luxembourg/
GEOJSON = 'https://download.data.public.lu/resources/limites-administratives-du-grand-duche-de-luxembourg/20180913-143737/lu-limites-administratives-2018.geojson'


@district.extractor(GEOJSON, layer='districts')  # NOQA
def extract_lu_district(db, polygon):
    '''
    Extract a luxembourgish district informations from a MultiPolygon.
    Based on data from:
    https://data.public.lu/fr/datasets/limites-administratives-du-grand-duche-de-luxembourg/
    '''
    props = polygon['properties']
    code = props['ISO'].lower()
    return {
        'code': code,
        'name': props['NOM'],
        'area': float(props['SUPERFICIE'].replace(',', '.')),
        'population': int(props['POPULATION']),
        'density': float(props['DENSITE'].replace(',', '.')),
        'parents': ['country:lu', 'country-group:ue', 'country-group:world'],
        'keys': {
            'iso': code,
        },
        'validity': {
            'start': props['CREATED'],
            'end': props['DELETED'],
        }
    }


@canton.extractor(GEOJSON, layer='cantons')  # NOQA
def extract_lu_canton(db, polygon):
    '''
    Extract a luxembourgish canton informations from a MultiPolygon.
    Based on data from:
    https://data.public.lu/fr/datasets/limites-administratives-du-grand-duche-de-luxembourg/
    '''
    props = polygon['properties']
    code = props['ISO'].lower()

    parents = {'country:lu', 'country-group:ue', 'country-group:world'}

    district = db.find_one({'level': 'lu:district', 'name': props['DISTRICT']})

    if district:
        parents.add(district['_id'])
        parents |= set(district['parents'])

    return {
        'code': code,
        'name': props['NOM'],
        'area': float(props['SUPERFICIE'].replace(',', '.')),
        'population': int(props['POPULATION']),
        'density': float(props['DENSITE'].replace(',', '.')),
        'parents': list(parents),
        'keys': {
            'iso': code,
        }
    }

@commune.extractor(GEOJSON, layer='communes')  # NOQA
def extract_lu_commune(db, polygon):
    '''
    Extract a luxembourgish commune informations from a MultiPolygon.
    Based on data from:
    https://data.public.lu/fr/datasets/limites-administratives-du-grand-duche-de-luxembourg/
    '''
    props = polygon['properties']
    code = props['LAU2']

    parents = {'country:lu', 'country-group:ue', 'country-group:world'}

    canton = db.find_one({'level': 'lu:canton', 'name': props['CANTON']})

    if canton:
        parents.add(canton['_id'])
        parents |= set(canton['parents'])

    return {
        'code': code,
        'name': props['COMMUNE'],
        'parents': list(parents),
        'keys': {
            'lau2': code,
        }
    }
