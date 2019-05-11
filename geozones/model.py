# -*- coding: utf-8 -*-
import os
import traceback

from os.path import join, basename

from fiona.crs import to_string
from shapely.geometry import shape, MultiPolygon
from shapely.validation import explain_validity

from .loaders import load
from .tools import warning, error, info, success, progress
from .tools import aggregate_multipolygons, match_patterns


class Level(object):
    '''
    This class handle level declaration and processing.
    '''

    def __init__(self, id, label, admin_level, *parents):
        # TODO: handle multiple parents
        self.id = id
        self.label = label
        self.parents = parents
        self.admin_level = admin_level
        self.children = []
        self.preprocessors = []
        self.extractors = []
        self.postprocessors = []
        self.aggregates = []
        for parent in parents:
            parent.children.append(self)

    def __str__(self):
        return self.id


    def preprocessor(self, url=None, **kwargs):
        '''
        Register a non geospatial dataset and its processor.
        '''
        def wrapper(func):
            func.kwargs = kwargs
            self.preprocessors.append((url, func))
            return func
        return wrapper

    def extractor(self, url, simplify=None, **kwargs):
        '''
        Register a dataset and its extractor.

        The function should have the following signature:
        ``function(polygon)`` where polygon will be a Shapely extracted
        polygon with GeoJSON interface.
        It should return a dictionnary with extrated attributes, at least:

            - name
            - code

        The simplify parameter is documented here (we use `0.005` for France):
        http://toblerity.org/shapely/manual.html#object.simplify
        '''
        def wrapper(func):
            func.simplify = simplify
            func.kwargs = kwargs
            self.extractors.append((url, func))
            return func
        return wrapper

    def postprocessor(self, url=None, **kwargs):
        '''
        Register a non geospatial dataset and its processor.
        '''
        def wrapper(func):
            func.kwargs = kwargs
            self.postprocessors.append((url, func))
            return func
        return wrapper

    @property
    def downloads(self):
        '''The required datasets URLs list with their target filename.'''
        return [
            (url, self.filename_for(url, fn))
            for url, fn in self.preprocessors + self.extractors + self.postprocessors if url
        ]

    def filename_for(self, url, fn):
        '''Compute the target download filename for a given URL'''
        filename = fn.kwargs.get('filename', os.path.basename(url))
        return os.path.join(self.id, filename)

    def aggregate(self, id, label, zones, **properties):
        '''Register a aggregate for this level.'''
        self.aggregates.append((id, label, zones, properties))

    def traverse(self):
        '''Deep tree traversal.'''
        done = {self.id}
        yield self
        for child in self.children:
            for level in child.traverse():
                if level.id not in done:
                    yield level
                    done.add(level.id)

    def load(self, workdir, db, only=None, exclude=None):
        '''
        Extract territories from a given file for a given level
        with a given extractor function.
        '''
        loaded = 0
        for url, extractor in self.extractors:
            if only is not None and extractor.__name__ != only:
                continue
            if match_patterns(extractor.__name__, exclude):
                continue
            loaded += self.process_dataset(workdir, db, url, extractor)
        success('Loaded {0} zones for level {1}', loaded, self.id)
        return loaded

    def process_dataset(self, workdir, db, url, extractor):
        '''
        Extract territories from a given file for a given level
        with a given extractor function.
        '''
        loaded = 0
        filename = join(workdir, self.filename_for(url, extractor))
        layer = getattr(extractor, 'layer', None)

        with load(filename, **extractor.kwargs) as collection:
            if layer:
                msg = '{0}/{1} ({2} {3})'.format(
                     basename(filename), layer, collection.driver,
                     to_string(collection.crs))
            elif hasattr(collection, 'driver'):
                msg = '{0} ({1} {2})'.format(
                     basename(filename), collection.driver,
                     to_string(collection.crs))
            else:
                msg = '{0}'.format(basename(filename))

            for polygon in progress(collection, msg):
                try:
                    zone = extractor(db, polygon)
                    if not zone:
                        continue
                    zone['keys'] = dict(
                        (k, v)
                        for k, v in zone.get('keys', {}).items()
                        if v is not None)
                    geom = shape(polygon['geometry'])
                    if extractor.simplify:
                        geom = geom.simplify(extractor.simplify)
                    if geom.geom_type == 'Polygon':
                        geom = MultiPolygon([geom])
                    elif geom.geom_type != 'MultiPolygon':
                        warning('Unsupported geometry type "{0}" for "{1}"',
                                geom.geom_type, zone['name'])
                        continue
                    zone.update(geom=geom.__geo_interface__)
                    zone_id = zone.get('_id')
                    if not zone_id:
                        zone_id = ':'.join((self.id, zone['code']))
                        if 'validity' in zone and zone['validity'].get('start'):
                            start = zone['validity']['start']
                            zone_id = '@'.join((zone_id, start))

                    if not geom.is_valid:
                        warning('Invalid geometry for "{0}": {1}', zone_id, explain_validity(geom))

                    zone.update(_id=zone_id, level=self.id)
                    db.find_one_and_replace(
                        {'_id': zone_id}, zone, upsert=True)
                    loaded += 1
                except Exception:
                    props = dict(polygon.get('properties', {}))
                    error('Error extracting polygon {0}:\n{1}',
                          props, traceback.format_exc())

        info('Loaded {0} zones for level {1} from file {2}',
             loaded, self.id, filename)
        return loaded

    def build_aggregates(self, db):
        processed = 0
        for code, name, zones, properties in self.aggregates:
            info('Building aggregate "{0}" (level={1}, code={2})',
                 name, self.id, code)
            if callable(zones):
                zones = zones(db)
            zone = self.build_aggregate(code, name, zones, properties, db)
            db.find_one_and_replace({'_id': zone['_id']}, zone, upsert=True)
            processed += 1
        return processed

    def build_aggregate(self, code, name, zones, properties, db):
        geoid = ':'.join((self.id, code))
        geoms = []
        populations = []
        areas = []
        if callable(zones):
            zones = [zone['_id'] for zone in zones(db)]
        for zoneid in progress(zones, 'Building {}'.format(geoid)):
            # Resolve wildcard
            if zoneid.endswith(':*'):
                level = zoneid.replace(':*', '')
                ids = db.distinct('_id', {'level': level})
                resolved = self.build_aggregate(
                    code, name, ids, properties, db)
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
                if 'geom' not in zone:
                    warning('Zone {0} without geometry'.format(zone['name']))
                    continue
                shp = shape(zone['geom'])
                if not shp.is_valid:
                    warning(('Skipping invalid polygon for {0}'
                             '').format(zone['name']))
                    continue
                if shp.is_empty:
                    warning('Skipping empty polygon for {0}', zone['name'])
                    continue
                geoms.append(shp)
                if zone.get('population'):
                    populations.append(zone['population'])
                if zone.get('area'):
                    areas.append(zone['area'])

        if geoms:
            geom = aggregate_multipolygons(geoms).__geo_interface__
        else:
            geom = None
            warning('No geometry for {0}', zones)

        data = {
            '_id': geoid,
            'code': code,
            'level': self.id,
            'name': name,
            'population': sum(populations),
            'area': sum(areas),
            'geom': geom
        }
        data.update(properties)
        return data

    def preprocess(self, workdir, db, only=None, exclude=None):
        '''Perform preprocessing.'''
        self._process(self.preprocessors, workdir, db, only=only, exclude=exclude)

    def postprocess(self, workdir, db, only=None, exclude=None):
        '''Perform postprocessing.'''
        self._process(self.postprocessors, workdir, db, only=only, exclude=exclude)

    def _process(self, lst, workdir, db, only=None, exclude=None):
        '''Perform postprocessing.'''
        for url, processor in lst:
            if only is not None and processor.__name__ != only:
                continue
            if match_patterns(processor.__name__, exclude):
                continue
            if url:
                filename = self.filename_for(url, processor)
                filename = os.path.join(workdir, filename)
                with load(filename, **processor.kwargs) as collection:
                    processor(db, collection)
            else:
                processor(db)


# Force translatables string extraction
_ = lambda s: s  # noqa
# Register first levels
root = country_group = Level('country-group', _('Country group'), 10)
country = Level('country', _('Country'), 20, country_group)
country_subset = Level('country-subset', _('Country subset'), 30, country)
