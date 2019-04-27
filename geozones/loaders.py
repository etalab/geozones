import csv
import json
# import os

from contextlib import contextmanager
from zipfile import ZipFile

import fiona


_loaders = {}


def loader(ext):
    '''Register an loader for a given extension'''
    def wrapper(func):
        _loaders[ext] = contextmanager(func)
        return func
    return wrapper


@contextmanager
def load(fname, **kwargs):
    '''FInd a loader for the longuest matching extension'''
    loaders = sorted(_loaders.items(), key=lambda i: len(i[0]))
    loaders = (l for e, l in loaders if fname.endswith(e))
    loader = next(loaders, None)
    if loader:
        with loader(fname, **kwargs) as collection:
            yield collection
    else:
        yield fname


@loader('.zip')
def load_shp_zip(fname, encoding='latin-1', **kwargs):
    # Identify the shapefile to avoid multiple file error on GDAL 2
    with ZipFile(fname) as z:
        candidates = [n for n in z.namelist() if n.endswith('.shp')]
        if len(candidates) > 1:
            # Try the exact name of the zip file for arrondissements
            # given that the epci shapefile is present too...
            for candidate in candidates:
                # Remove useless prefix and suffix.
                guessed_name = fname[len('downloads/'):-len('-shp.zip')]
                if guessed_name in candidate:
                    candidates = [candidate]
                    break
        if len(candidates) != 1:
            msg = 'Unable to find a unique shapefile into {0} {1}'
            raise ValueError(msg.format(fname, candidates))
        shp = candidates[0]

    with fiona.open('/{0}'.format(shp),
                    vfs='zip://{0}'.format(fname),
                    encoding=encoding) as collection:
        yield collection


@loader('.geojson')
def load_geojson(fname, layer=None, **kwargs):
    with fiona.open(fname, layer=layer) as collection:
        yield collection


@loader('.geojson.gz')
def load_gzipped_geojson(fname, layer=None, **kwargs):
    with fiona.open('gzip://{0}'.format(fname), layer=layer) as collection:
        yield collection


@loader('.json')
def load_json(fname, **kwargs):
    with open(fname) as infile:
        yield json.load(infile)


@loader('csv')
def load_csv(fname, encoding='utf8', delimiter=',', quotechar='"', **kwargs):
    with open(fname) as infile:
        reader = csv.DictReader(infile, delimiter=delimiter, quotechar=quotechar)
        yield reader
