import fiona
import json

from tools import unicodify


def zone_to_feature(zone):
    '''Serialize a zone into a GeoJSON feature'''
    return {
        'id': zone['_id'],
        'type': 'Feature',
        'geometry': zone['geom'],
        'properties': {
            'level': zone['level'],
            'code': zone['code'],
            'name': unicodify(zone['name']),
            'wikipedia': unicodify(zone.get('wikipedia', '')) or None,
            'dbpedia': unicodify(zone.get('dbpedia', '')) or None,
            'population': zone.get('population', None),
            'area': zone.get('area', None),
            'flag': unicodify(zone.get('flag', '')) or None,
            'blazon': unicodify(zone.get('blazon', '')) or None,
            'keys': zone.get('keys', {}),
            'parents': zone.get('parents', [])
        }
    }


def dump_zones(zones):
    '''Serialize a zones queryset into a sarializable dict'''
    features = [zone_to_feature(z) for z in zones]
    data = {
        'type': 'FeatureCollection',
        'features': features,
        'crs': fiona.crs.from_epsg(4326)
    }
    return data


def dumps(zones, pretty=False):
    data = dump_zones(zones)
    if pretty:
        return json.dumps(data, indent=4)
    else:
        return json.dumps(data)


def dump(zones, out, pretty=False):
    data = dump_zones(zones)
    if pretty:
        return json.dump(data, out, indent=4)
    else:
        return json.dump(data, out)
