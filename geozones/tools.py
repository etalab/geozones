import csv
import io

from contextlib import contextmanager
from itertools import islice
from os.path import basename
from urllib.request import urlopen, Request
from zipfile import ZipFile

import click

from shapely.geometry import shape, MultiPolygon
from shapely.ops import cascaded_union


def _secho(prefix=None, **style):
    def func(text='', *args, **kwargs):
        text = text.strip().format(*args, **kwargs)
        if '\n' in text:
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
INFO = click.style('⦿', fg='cyan')
HOURGLASS = click.style('⏳', fg='cyan')

title = _secho(ARROW, fg='white', bold=True)
section = _secho(DOUBLE_ARROW, fg='white', bold=True)
info = _secho(INFO, bold=True)
success = _secho(OK, fg='white', bold=True)
error = _secho(KO, fg='red', bold=True)
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


def progress(collection, msg):
    label = ' '.join((HOURGLASS, msg))
    with click.progressbar(collection, label=label) as bar:
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
    content_disposition = response.headers.get('Content-Disposition', '')
    if 'filename' in content_disposition:
        # Retrieve the filename and remove the last ".
        filename = content_disposition.split('filename="')[-1][:-1]
    else:
        filename = basename(url).strip()

    content_length = response.headers.get('Content-Length')
    if content_length:
        size = int(content_length)
    else:
        size = 1  # Fake for progress bar.

    return filename, size


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
    aggregated = cascaded_union(multipolygons)
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
