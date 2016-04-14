# -*- coding: utf-8 -*-
from os.path import join, basename

import fiona

from fiona.crs import to_string
from shapely.geometry import shape, MultiPolygon
from shapely.ops import cascaded_union

from tools import warning, error, info, success, extract_meta_from_headers


class Level(object):
    '''This class handle level declaration and processing'''

    def __init__(self, id, label, *parents):
        # TODO: handle multiple parents
        self.id = id
        self.label = label
        self.parents = parents
        self.children = []
        self.extractors = []
        self.postprocessors = []
        self.aggregates = []
        for parent in parents:
            parent.children.append(self)

    def extractor(self, url, simplify=None):
        '''
        Register a dataset and its extractor

        The function should have the following signature: ``function(polygon)``
        where polygon will be a Shapely extracted polygon with GeoJSON interface.
        It should return a dictionnary with extrated attributes, at least:

            - name
            - code

        The simplify parameter is documented here (we use `0.005` for France):
        http://toblerity.org/shapely/manual.html#object.simplify
        '''
        def wrapper(func):
            func.simplify = simplify
            self.extractors.append((url, func))
            return func
        return wrapper

    def postprocessor(self, url=None):
        '''
        Register a non geospatial dataset and its processor.
        '''
        def wrapper(func):
            self.postprocessors.append((url, func))
            return func
        return wrapper

    @property
    def urls(self):
        '''The required datasets URLs list'''
        return [url for url, _ in self.extractors + self.postprocessors if url]

    def aggregate(self, id, label, zones, **properties):
        '''Register a aggregate for this level'''
        self.aggregates.append((id, label, zones, properties))

    def traverse(self):
        '''Deep tree traversal'''
        levels = [self]
        children = []
        while len(levels) > 0:
            for level in levels:
                yield level
                children.extend(level.children)
            levels, children = children, []

    def load(self, workdir, db):
        '''Extract territories from a given file for a given level with a given extractor function'''
        loaded = 0
        for url, extractor in self.extractors:
            loaded += self.process_dataset(workdir, db, url, extractor)
        success('Loaded {0} zones for level {1}'.format(loaded, self.id))
        return loaded

    def process_dataset(self, workdir, db, url, extractor):
        '''Extract territories from a given file for a given level with a given extractor function'''
        loaded = 0
        filename = join(workdir, basename(url))

        with fiona.open('/', vfs='zip://{0}'.format(filename), encoding='utf8') as collection:
            info('Extracting {0} elements from {1} ({2} {3})'.format(
                len(collection), basename(filename), collection.driver, to_string(collection.crs)
            ))

            for polygon in collection:
                try:
                    zone = extractor(polygon)
                    if not zone:
                        continue
                    zone['keys'] = dict((k, v) for k, v in zone.get('keys', {}).items() if v is not None)
                    geom = shape(polygon['geometry'])
                    if extractor.simplify:
                        geom = geom.simplify(extractor.simplify)
                    if geom.geom_type == 'Polygon':
                        geom = MultiPolygon([geom])
                    elif geom.geom_type != 'MultiPolygon':
                        warning('Unsupported geometry type "{0}" for "{1}"'.format(geom.geom_type, zone['name']))
                        continue
                    zoneid = '/'.join((self.id, zone['code']))
                    zone.update(_id=zoneid, level=self.id, geom=geom.__geo_interface__)
                    db.find_one_and_replace({'_id': zoneid}, zone, upsert=True)
                    loaded += 1
                except Exception as e:
                    error('Error extracting polygon {0}: {1}', polygon['properties'], str(e))

        info('Loaded {0} zones for level {1} from file {2}'.format(loaded, self.id, filename))
        return loaded

    def build_aggregates(self, db):
        processed = 0
        for code, name, zones, properties in self.aggregates:
            info('Building aggregate "{0}" (level={1}, code={2})'.format(name, self.id, code))
            zone = self.build_aggregate(code, name, zones, properties, db)
            db.find_one_and_replace({'_id': zone['_id']}, zone, upsert=True)
            processed += 1
        return processed

    def build_aggregate(self, code, name, zones, properties, db):
        geoms = []
        populations = []
        areas = []
        for zoneid in zones:
            # Resolve wildcard
            if zoneid.endswith('/*'):
                level = zoneid.replace('/*', '')
                ids = db.distinct('_id', {'level': level})
                resolved = self.build_aggregate(code, name, ids, properties, db)
                geoms.append(shape(resolved['geom']))
                if resolved.get('population'):
                    populations.append(resolved['population'])
                if resolved.get('area'):
                    areas.append(resolved['area'])
            else:
                zone = db.find_one({'_id': zoneid})
                if not zone:
                    warning('Zone {0} not found'.format(zoneid))
                    continue
                shp = shape(zone['geom'])
                if not shp.is_valid:
                    warning('Skipping invalid polygon for {0}'.format(zone['name']))
                    continue
                if shp.is_empty:
                    warning('Skipping empty polygon for {0}'.format(zone['name']))
                    continue
                geoms.append(shp)
                if zone.get('population'):
                    populations.append(zone['population'])
                if zone.get('area'):
                    areas.append(zone['area'])

        geom = cascaded_union(geoms)
        if geom.geom_type == 'Polygon':
            geom = MultiPolygon([geom])

        data = {
            '_id': '/'.join((self.id, code)),
            'code': code,
            'level': self.id,
            'name': name,
            'population': sum(populations),
            'area': sum(areas),
            'geom': geom.__geo_interface__
        }
        data.update(properties)
        return data

    def postprocess(self, workdir, db, only=None):
        '''Perform postprocessing'''
        for url, processor in self.postprocessors:
            if only is not None and processor.__name__ != only:
                continue
            filepath = None
            if url:
                filename, _ = extract_meta_from_headers(url)
                filepath = join(workdir, filename)
            processor(db, filepath)


# Force translatables string extraction
_ = lambda s: s  # noqa
# Register first levels
root = country_group = Level('country-group', _('Country group'))
country = Level('country', _('Country'), country_group)
country_subset = Level('country-subset', _('Country subset'), country)
