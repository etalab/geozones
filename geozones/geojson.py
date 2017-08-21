import fiona
import json

from .tools import unicodify


def zone_to_feature(zone, keys=None):
    '''Serialize a zone into a GeoJSON feature'''
    properties = {
        'level': zone['level'],
        'code': zone['code'],
        'name': unicodify(zone['name']),
        'wikipedia': unicodify(zone.get('wikipedia', '')) or None,
        'dbpedia': unicodify(zone.get('dbpedia', '')) or None,
        'population': int(zone.get('population', 0)),
        'area': int(zone.get('area', 0)),
        'flag': unicodify(zone.get('flag', '')) or None,
        'blazon': unicodify(zone.get('blazon', '')) or None,
        'keys': zone.get('keys', {}),
        'validity': zone.get('validity', {}),
        'parents': zone.get('parents', '') or None,
        'ancestors': zone.get('ancestors', '') or None,
        'successors': zone.get('successors', '') or None,
    }
    if keys is not None:
        for unwanted_key in set(properties.keys()) - set(keys):
            del properties[unwanted_key]
    feature = {
        'id': zone['_id'],
        'type': 'Feature',
        'geometry': zone.get('geom'),
        'properties': properties
    }
    if keys is not None and 'geometry' not in keys:
        del feature['geometry']
    return feature


def dump_zones(zones, keys=None):
    '''Serialize a zones queryset into a sarializable dict'''
    features = [zone_to_feature(z, keys) for z in zones]
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


def dump(zones, out, pretty=False, keys=None):
    data = dump_zones(zones, keys=keys)
    if pretty:
        return json.dump(data, out, indent=4)
    else:
        return json.dump(data, out)
