# -*- coding: utf-8 -*-
import traceback
from contextlib import contextmanager
from os.path import join, basename
from zipfile import ZipFile

import fiona
from fiona.crs import to_string
from shapely.geometry import shape, MultiPolygon

from .tools import warning, error, info, success, extract_meta_from_headers
from .tools import aggregate_multipolygons


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

    def preprocessor(self, url=None):
        '''
        Register a non geospatial dataset and its processor.
        '''
        def wrapper(func):
            self.preprocessors.append((url, func))
            return func
        return wrapper

    def extractor(self, url, simplify=None, layer=None):
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
            func.layer = layer
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
        '''The required datasets URLs list.'''
        return [url for url, _ in self.extractors + self.postprocessors if url]

    def aggregate(self, id, label, zones, **properties):
        '''Register a aggregate for this level.'''
        self.aggregates.append((id, label, zones, properties))

    def traverse(self):
        '''Deep tree traversal.'''
        levels = [self]
        children = []
        while len(levels) > 0:
            for level in levels:
                yield level
                children.extend(level.children)
            levels, children = children, []

    def load(self, workdir, db, only=None, exclude=None):
        '''
        Extract territories from a given file for a given level
        with a given extractor function.
        '''
        loaded = 0
        for url, extractor in self.extractors:
            if only is not None and extractor.__name__ != only:
                continue
            if exclude is not None and extractor.__name__ == exclude:
                continue
            loaded += self.process_dataset(workdir, db, url, extractor)
        success('Loaded {0} zones for level {1}', loaded, self.id)
        return loaded

    @contextmanager
    def load_shp_zip(self, filename, layer=None):
        # Identify the shapefile to avoid multiple file error on GDAL 2
        with ZipFile(filename) as z:
            candidates = [n for n in z.namelist() if n.endswith('.shp')]
            if len(candidates) > 1:
                # Try the exact name of the zip file for arrondissements
                # given that the epci shapefile is present too...
                for candidate in candidates:
                    # Remove useless prefix and suffix.
                    guessed_name = filename[len('downloads/'):-len('-shp.zip')]
                    if guessed_name in candidate:
                        candidates = [candidate]
                        break
            if len(candidates) != 1:
                msg = 'Unable to find a unique shapefile into {0} {1}'
                raise ValueError(msg.format(filename, candidates))
            shp = candidates[0]

        with fiona.open('/{0}'.format(shp),
                        vfs='zip://{0}'.format(filename),
                        encoding='latin-1') as collection:
            yield collection

    @contextmanager
    def load_geojzon(self, filename, layer=None):
        with fiona.open(filename, layer=layer) as collection:
            yield collection

    def process_dataset(self, workdir, db, url, extractor):
        '''
        Extract territories from a given file for a given level
        with a given extractor function.
        '''
        loaded = 0
        filename = join(workdir, basename(url))
        layer = extractor.layer

        info('processing {0}', basename(filename))

        if filename.endswith('.zip'):
            loader = self.load_shp_zip
        elif filename.endswith('.geojson'):
            loader = self.load_geojzon

        with loader(filename, layer=layer) as collection:
            if layer:
                info('Extracting {0} elements from {1} => {2} ({3} {4})',
                     len(collection), basename(filename), layer, collection.driver,
                     to_string(collection.crs))
            else:
                info('Extracting {0} elements from {1} => ({2} {3})',
                     len(collection), basename(filename), collection.driver,
                     to_string(collection.crs))

            for polygon in collection:
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
                    zone.update(_id=zone_id, level=self.id)
                    db.find_one_and_replace(
                        {'_id': zone_id}, zone, upsert=True)
                    loaded += 1
                except Exception:
                    error('Error extracting polygon {0}',
                          polygon['properties'])
                    error(traceback.format_exc())

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
        geoms = []
        populations = []
        areas = []
        if callable(zones):
            zones = [zone['_id'] for zone in zones(db)]
        for zoneid in zones:
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
            '_id': ':'.join((self.id, code)),
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
        for url, processor in self.preprocessors:
            if only is not None and processor.__name__ != only:
                continue
            if exclude is not None and processor.__name__ == exclude:
                continue
            filepath = None
            if url:
                filename, _ = extract_meta_from_headers(url)
                filepath = join(workdir, filename)
            processor(db, filepath)

    def postprocess(self, workdir, db, only=None, exclude=None):
        '''Perform postprocessing.'''
        for url, processor in self.postprocessors:
            if only is not None and processor.__name__ != only:
                continue
            if exclude is not None and processor.__name__ == exclude:
                continue
            filepath = None
            if url:
                filename, _ = extract_meta_from_headers(url)
                filepath = join(workdir, filename)
            processor(db, filepath)


# Force translatables string extraction
_ = lambda s: s  # noqa
# Register first levels
root = country_group = Level('country-group', _('Country group'), 10)
country = Level('country', _('Country'), 20, country_group)
country_subset = Level('country-subset', _('Country subset'), 30, country)
