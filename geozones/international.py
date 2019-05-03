from .model import country, country_group
from .tools import info, warning, success

_ = lambda s: s  # noqa: E731


# This is the latest URL (currently 4.1.0)
# There is no versionned adress right now
NE_URL = 'https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/110m/cultural/ne_110m_admin_0_countries_lakes.zip'

# Natural Earth stores None as '-99'
NE_NONE = '-99'

# Associate some fixes to Natural Earth polygons.
# Use the `NE_ID` attribute to identify each polygon
NE_FIXES = {
    # France is lacking ISO codes
    1159320637: {
        'ISO_A2': 'FR',
        'ISO_A3': 'FRA',
    },
    # Norway is lacking ISO codes
    1159321109: {
        'ISO_A2': 'NO',
        'ISO_A3': 'NOR',
    }
}


def ne_prop(props, key, cast=str):
    '''
    Fetch a Natural Eartch property.

    This method handle:
    - both upper and lower case as casing has been changing between releases
    - try with a trailing underscore (some properties are moving)
    - returns `None` instead of `-99`
    - match fixes if any
    - perform a cast if necessary
    - case lowering
    '''
    ne_id = props['NE_ID']
    upper_key = key.upper()
    keys = (upper_key, key.lower(), upper_key + '_', key.lower() + '_')
    value = None
    for key in keys:
        if key in props:
            value = props[key]
            continue
    if value == NE_NONE:  # None value for Natural Earth
        if ne_id in NE_FIXES and upper_key in NE_FIXES[ne_id]:
            value = NE_FIXES[ne_id][upper_key]
        else:
            return None
    if not value:
        return None
    elif cast is str:
        return value.lower()
    else:
        return cast(value)



@country.extractor(NE_URL, encoding='utf-8')  # NOQA
def extract_country(db, polygon):
    '''
    Extract a country information from single MultiPolygon.
    Based on data from:
    http://www.naturalearthdata.com/downloads/110m-cultural-vectors/110m-admin-0-countries/

    The main unique code used is ISO2.
    '''
    props = polygon['properties']
    code = ne_prop(props, 'ISO_A2')
    if not code:
        warning('Missing iso code 2 for {NAME}, skipping'.format(**props))
        return
    return {
        'code': code,
        'name': props['NAME'],
        'population': ne_prop(props, 'POP_EST', int),
        'parents': ['country-group:world'],
        'keys': {
            'iso2': code,
            'iso3': ne_prop(props, 'ISO_A3'),
            'un': ne_prop(props, 'UN_A3'),
            'fips': ne_prop(props, 'FIPS_10'),
        }
    }


@country.extractor('https://datahub.io/core/geo-countries/r/countries.geojson')
def extract_countries(db, polygon):
    '''
    Use cleaner shapes from Datahub geo countries: https://datahub.io/core/geo-countries
    '''
    props = polygon['properties']
    return next(db.level(country.id, **{'keys.iso3': props['ISO_A3'].lower()}), None)


# World Aggregate
country_group.aggregate(
    'world', _('World'), ['country:*'], keys={'default': 'world'})


# European union
UE_COUNTRIES = (
    'at', 'be', 'bg', 'cy', 'hr', 'dk', 'ee', 'fi', 'gr', 'fr', 'es', 'de',
    'hu', 'ie', 'it', 'lv', 'lt', 'lu', 'mt', 'nl', 'no', 'pl', 'pt', 'cz',
    'ro', 'gb', 'sk', 'si', 'se'
)

country_group.aggregate(
    'ue', _('European Union'),
    ['country:{0}'.format(code) for code in UE_COUNTRIES],
    parents=['country-group:world'],
    keys={'default': 'ue'},
    wikipedia='en:European_Union',
    wikidata='Q458',
)


@country.postprocessor()
def add_ue_to_parents(db):
    info('Adding European Union to countries parents')
    result = db.update_many(
        {'level': country.id, 'code': {'$in': UE_COUNTRIES}},
        {'$addToSet': {'parents': 'country-group:ue'}})
    success('Added European Union as parent to {0} countries',
            result.modified_count)
