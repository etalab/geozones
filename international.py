from geo import country, country_group
from tools import info, success

_ = lambda s: s


@country.extractor('http://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries_lakes.zip')
def extract_country2(polygon):
    '''
    Extract a country information from single MultiPolygon.
    Based on data from http://www.naturalearthdata.com/downloads/110m-cultural-vectors/110m-admin-0-countries/

    The main unique code used is ISO2.
    '''
    props = polygon['properties']
    code = props['iso_a2'].lower()
    return {
        'code': code,
        'name': props['name'],
        'population': int(props['pop_est']),
        'parents': ['country-group/world'],
        'keys': {
            'iso2': code,
            'iso3': props['iso_a3'].lower(),
            'un': props['un_a3'],
            'fips': (props.get('fips_10', '') or '').lower() or None,
        }
    }


# World Aggregate
country_group.aggregate(
    'world', _('World'), ['country/*'], keys={'default': 'world'})


# European union
UE_COUNTRIES = (
    'at', 'be', 'bg', 'cy', 'hr', 'dk', 'ee', 'fi', 'gr', 'fr', 'es', 'de',
    'hu', 'ie', 'it', 'lv', 'lt', 'lu', 'mt', 'nl', 'pl', 'pt', 'cz', 'ro',
    'gb', 'sk', 'si', 'se'
)

country_group.aggregate('ue', _('European Union'), [
    'country/{0}'.format(code) for code in UE_COUNTRIES
], parents=['country-group/world'], keys={'default': 'ue'})


@country.postprocessor()
def add_ue_to_parents(db, filename):
    info('Adding European Union to countries parents')
    result = db.update_many(
        {'level': country.id, 'code': {'$in': UE_COUNTRIES}},
        {'$addToSet': {'parents': 'country-group/ue'}})
    success(('Added European Union as parent to {0} countries'
             '').format(result.modified_count))
