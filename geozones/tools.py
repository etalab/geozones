import csv
import inspect
import io

from collections import Iterator
from contextlib import contextmanager
from itertools import islice, tee
from os.path import basename
from urllib.request import urlopen, Request
from zipfile import ZipFile

import click

from shapely.geometry import shape, MultiPolygon
from shapely.ops import unary_union


def _secho(prefix=None, verbose=False, **style):
    def func(text='', *args, **kwargs):
        text = text.strip().format(*args, **kwargs)
        if not verbose and '\n' in text:
            text, _ = text.split('\n', 1)
        text = click.style(text.strip(), **style)
        if prefix:
            text = ' '.join((prefix, text))
        click.echo(text)
    return func


OK = click.style('✔', fg='green', bold=True)
KO = click.style('✘', fg='red')
WARNING = click.style('⚠️ ', fg='yellow')
ARROW = click.style('➤', fg='blue')
DOUBLE_ARROW = click.style('➤➤', fg='magenta')
INFO = click.style('⚙', fg='cyan')
PROGRESS = click.style('⏱', fg='cyan')
PROGRESS_FILL_CHAR = click.style('◼', fg='cyan')

title = _secho(ARROW, fg='white', bold=True)
section = _secho(DOUBLE_ARROW, fg='white', bold=True)
info = _secho(INFO, bold=True)
success = _secho(OK, fg='white', bold=True)
error = _secho(KO, verbose=True, fg='red', bold=True)
warning = _secho(WARNING, fg='yellow')


@contextmanager
def ok(text):
    try:
        text = click.style(text, bold=True)
        click.secho('{0} {1} ...... '.format(INFO, text), nl=False)
        yield
    except Exception:
        error()
        raise
    else:
        success()


def progress(collection, msg=None, length=True):
    label = ' '.join((PROGRESS, msg)) if msg else PROGRESS
    kwargs = {'label': label, 'width': 0, 'fill_char': PROGRESS_FILL_CHAR}
    if length is True and (inspect.isgenerator(collection) or isinstance(collection, Iterator)):
        collection, tmp = tee(collection)
        kwargs['length'] = sum(1 for _ in tmp)  # Don't use list(), it will waste memory
    elif length:
        # It's an integer
        kwargs['length'] = length

    with click.progressbar(collection, **kwargs) as bar:
        for item in bar:
            yield item


def unicodify(string):
    '''Ensure a string is unicode and serializable'''
    return (string.decode('unicode_escape')
            if isinstance(string, bytes) else string)


def convert_from(string, charset):
    '''Convert a string from a given charset to utf-8'''
    if not string:
        return
    return string.encode(charset).decode('utf-8')


def extract_meta_from_headers(url):
    """Given a `url`, perform a HEAD request and return metadata."""
    req = Request(url, method='HEAD')
    req.add_header('Accept-Encoding', 'identity')
    response = urlopen(req)
    content_disposition = response.getheader('Content-Disposition', '')
    meta = {}
    if 'filename' in content_disposition:
        # Retrieve the filename and remove the last ".
        meta['filename'] = content_disposition.split('filename="')[-1][:-1]
    else:
        meta['filename'] = basename(url).strip()

    content_length = response.getheader('Content-Length')
    if content_length:
        meta['size'] = int(content_length)

    content_type = response.getheader('Content-Type')
    if content_type:
        meta['mime'] = content_type.split(';', 1)[0]

    return meta


def iter_over_cog(zipname, filename):
    with ZipFile(zipname) as cogzip:
        with cogzip.open(filename) as tsvfile:
            tsvio = io.TextIOWrapper(tsvfile, encoding='cp1252')
            reader = csv.DictReader(tsvio, delimiter='\t')
            for row in reader:
                yield row


def geom_to_multipolygon(geom):
    '''Cast a raw geometry to a Polygon or a MultiPolygon'''
    polygon = shape(geom)
    if not polygon.is_valid:
        raise ValueError('Invalid polygon')
    elif polygon.is_empty:
        raise ValueError('Empty polygon')
    if polygon.geom_type == 'Polygon':
        polygon = MultiPolygon([polygon])
    elif polygon.geom_type != 'MultiPolygon':
        msg = 'Unsupported geometry type "{0}"'.format(polygon.geom_type)
        raise ValueError(msg)
    return polygon


def aggregate_multipolygons(multipolygons):
    '''Aggregate a list of multipolygons into a single multipolygon'''
    aggregated = unary_union(multipolygons)
    if aggregated.geom_type == 'Polygon':
        aggregated = MultiPolygon([aggregated])
    return aggregated


def chunker(iterator, size):
    '''Chunk an iterator into multiple iterator with a given size'''
    it = iter(iterator)
    while True:
        chunk = tuple(islice(it, size))
        if not chunk:
            return
        yield chunk
