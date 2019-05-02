#!/usr/bin/env python
import json
import os
import tarfile
import textwrap
import urllib
from urllib.request import FancyURLopener

import click
from pymongo import ASCENDING
import msgpack

from .db import DB
from .tools import (
    info, success, title, ok, error, section, warning, _secho,
    extract_meta_from_headers, PROGRESS, PROGRESS_FILL_CHAR
)
from .model import root
from .logos import fetch_logos, compress_logos
from .france.histo import (
    load_communes, load_departements, load_collectivites, load_regions,
    load_epcis, URLS as GEOHISTO_URLS
)
from . import geojson

# Importing levels modules in order (international first)
from . import international  # noqa
from . import france  # noqa
from . import luxembourg  # noqa

DL_DIR = 'downloads'
DIST_DIR = 'dist'
CONTEXT_SETTINGS = {
    'help_option_names': ['-?', '--help'],
    'auto_envvar_prefix': 'GEOZONES',
}

urlretrieve = FancyURLopener().retrieve


def downloadable_urls(ctx):
    urls = (level.downloads for level in ctx.obj['levels'] if level.downloads)
    # return set([url for lst in urls for url in lst] + GEOHISTO_URLS)
    return set([url for lst in urls for url in lst])


@click.group(chain=True, context_settings=CONTEXT_SETTINGS)
@click.option('-d', '--drop', is_flag=True)
@click.option('-l', '--level', multiple=True, help='Limits to given levels')
@click.option('-e', '--exclude', multiple=True, help='Exclude some levels')
@click.option('-m', '--mongo', help='MongoDB database', default='localhost')
@click.option('-H', '--home', help='Specify GeoZones working home')
@click.pass_context
def cli(ctx, drop, level, exclude, mongo, home):
    ctx.obj = {}
    if home:
        os.chdir(home)
    else:
        home = os.getcwd()
    ctx.obj['home'] = home

    levels = []
    for l in root.traverse():
        should_process = (not level or l.id in level) and l.id not in exclude
        if should_process and l not in levels:
            levels.append(l)

    ctx.obj['levels'] = levels

    db = ctx.obj['db'] = DB(mongo)

    if drop:
        with ok('Droping existing collection'):
            db.drop()

    with ok('Initializing collection'):
        db.initialize()


@cli.command()
@click.pass_context
def download(ctx):
    '''
    Download required datasets (~700Mb).

    Take about 15 minutes depending on your connexion bandwidth.
    '''
    title(textwrap.dedent(download.__doc__))
    if not os.path.exists(DL_DIR):
        os.makedirs(DL_DIR)

    for url, filename in downloadable_urls(ctx):
        target = os.path.join(DL_DIR, filename)
        if os.path.exists(target):
            info('Skipping {0} because it already exists.'.format(filename))
            continue

        info('Processing {0}'.format(filename))
        target_dir = os.path.dirname(target)

        try:
            meta = extract_meta_from_headers(url)
        except urllib.error.HTTPError:
            warning('Error with URL {0}.'.format(url))
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        info('Downloading {0}'.format(url))
        size = meta.get('size')
        with click.progressbar(length=size, width=0, label=PROGRESS, fill_char=PROGRESS_FILL_CHAR) as bar:
            def reporthook(blocknum, blocksize, totalsize):
                read = blocknum * blocksize
                if read <= 0:
                    return
                bar.update(blocksize)

            urlretrieve(url, target, reporthook=reporthook)


@cli.command()
@click.pass_context
def sourceslist(ctx):
    '''Generate a datasets donwload list for external usage'''
    print('\n'.join(l for l, _ in downloadable_urls(ctx)))


@cli.command()
@click.option('-d', '--drop', is_flag=True)
@click.pass_context
def preload(ctx, drop):
    '''
    Preload all historical zones from geohisto.

    Take a few seconds.
    '''
    title(textwrap.dedent(preload.__doc__))
    zones = ctx.obj['db']
    if drop:
        info('Drop existing collection')
        zones.drop()

    with ok('Creating index (level,code)'):
        zones.create_index([('level', ASCENDING), ('code', ASCENDING)])
    info('Creating index (level,keys)')
    zones.create_index([('level', ASCENDING), ('keys', ASCENDING)])
    info('Creating index (parents)')
    zones.create_index('parents')

    info('Load regions')
    total = load_regions(zones, DL_DIR)
    success('Done: Loaded {0} regions'.format(total))
    info('Load counties')
    total = load_departements(zones, DL_DIR)
    success('Done: Loaded {0} counties'.format(total))
    info('Load overseas collectivities')
    total = load_collectivites(zones, DL_DIR)
    success('Done: Loaded {0} overseas collectivities'.format(total))
    info('Load towns')
    total = load_communes(zones, DL_DIR)
    success('Done: Loaded {0} towns'.format(total))
    info('Load EPCIs')
    total = load_epcis(zones, DL_DIR)
    success('Done: Loaded {0} EPCIs'.format(total))


@cli.command()
@click.pass_context
@click.option('-o', '--only', default=None)
@click.option('-e', '--exclude', default=None)
def preprocess(ctx, only, exclude):
    '''
    Perform pre-processing.

    Excluding `fetch_missing_data_from_dbpedia` with the `-e`
    option will reduce the duration to 3 minutes.
    '''
    title(textwrap.dedent(preprocess.__doc__))
    zones = ctx.obj['db']

    for level in ctx.obj['levels']:
        level.preprocess(DL_DIR, zones, only, exclude)

    success('Pre-processing done')


@cli.command()
@click.pass_context
@click.option('-o', '--only', default=None)
@click.option('-e', '--exclude', default=None)
def load(ctx, only, exclude):
    '''
    Load zones from a folder of zip files containing shapefiles

    Take about 25 minutes.

    Excluding `extract_iris` with the `-e` option will reduce
    the duration to 10 minutes.
    '''
    title(textwrap.dedent(load.__doc__))
    zones = ctx.obj['db']
    total = 0

    for level in ctx.obj['levels']:
        section('Processing level "{0}"'.format(level.id))
        total += level.load(DL_DIR, zones, only, exclude)

    success('Done: Loaded {0} zones'.format(total))


@cli.command()
@click.pass_context
def aggregate(ctx):
    '''
    Perform zones aggregations.
    '''
    title(textwrap.dedent(aggregate.__doc__))
    zones = ctx.obj['db']

    total = 0

    for level in reversed(ctx.obj['levels']):
        total += level.build_aggregates(zones)

    success('Done: Built {0} zones by aggregation'.format(total))


@cli.command()
@click.pass_context
@click.option('-o', '--only', default=None)
@click.option('-e', '--exclude', default=None)
def postprocess(ctx, only, exclude):
    '''
    Perform post-processing.

    Take between 2 hours and 6 hours.

    Take care of the order, especially `process_insee_cog` and
    `compute_region_population` might need to be run again with
    the `-o` option.

    Excluding `fetch_missing_data_from_dbpedia` with the `-e`
    option will reduce the duration to 3 minutes.
    '''
    title(textwrap.dedent(postprocess.__doc__))
    zones = ctx.obj['db']

    for level in ctx.obj['levels']:
        level.postprocess(DL_DIR, zones, only, exclude)

    success('Post-processing done')


@cli.command()
@click.pass_context
@click.option('-p', '--pretty', is_flag=True)
@click.option('-s', '--split', is_flag=True)
@click.option('-c/-nc', '--compress/--no-compress', default=True)
@click.option('-r', '--serialization', default='json',
              type=click.Choice(['json', 'msgpack']))
@click.option('-k', '--keys', default=None)
def dist(ctx, pretty, split, compress, serialization, keys):
    '''Dump a distributable file'''
    keys = keys and keys.split(',')
    title('Dumping data to {serialization} with keys {keys}'.format(
        serialization=serialization, keys=keys))
    geozones = ctx.obj['db']
    filenames = []

    if not os.path.exists(DIST_DIR):
        os.makedirs(DIST_DIR)

    os.chdir(DIST_DIR)
    level_ids = [l.id for l in ctx.obj['levels']]

    if split:
        for level_id in level_ids:
            filename = 'zones-{level}.{serialization}'.format(
                level=level_id.replace(':', '-'), serialization=serialization)
            with ok('Generating {filename}'.format(filename=filename)):
                zones = geozones.find({'level': level_id, 'code': {'$exists': True}})
                if serialization == 'json':
                    with open(filename, 'w') as out:
                        geojson.dump(zones, out, pretty=pretty, keys=keys)
                else:
                    packer = msgpack.Packer(use_bin_type=True)
                    with open(filename, 'wb') as out:
                        for zone in zones:
                            out.write(packer.pack(zone))
            filenames.append(filename)
    else:
        filename = 'zones.{serialization}'.format(serialization=serialization)
        with ok('Generating {filename}'.format(filename=filename)):
            zones = geozones.find({'level': {'$in': level_ids}, 'code': {'$exists': True}})
            if serialization == 'json':
                with open(filename, 'w') as out:
                    geojson.dump(zones, out, pretty=pretty, keys=keys)
            else:
                packer = msgpack.Packer(use_bin_type=True)
                with open(filename, 'wb') as out:
                    for zone in zones:
                        out.write(packer.pack(zone))
        filenames.append(filename)

    filename = 'levels.{serialization}'.format(serialization=serialization)
    with ok('Generating {filename}'.format(filename=filename)):
        data = [{
            'id': level.id,
            'label': level.label,
            'admin_level': level.admin_level,
            'parents': [p.id for p in level.parents]
        } for level in ctx.obj['levels']]
        if serialization == 'json':
            with open(filename, 'w') as out:
                if pretty:
                    json.dump(data, out, indent=4)
                else:
                    json.dump(data, out)
        else:
            packer = msgpack.Packer(use_bin_type=True)
            with open(filename, 'wb') as out:
                for item in data:
                    out.write(packer.pack(item))
        filenames.append(filename)

    if compress:
        filename = 'geozones-translations.tar.xz'
        translations = os.path.join(ctx.obj['home'], 'geozones', 'translations')
        with ok('Compressing to {0}'.format(filename)):
            with tarfile.open(filename, 'w:xz') as txz:
                txz.add(translations, 'translations')

        filename = 'geozones{split}-{serialization}.tar.xz'.format(
            split='-split' if split else '', serialization=serialization)
        with ok('Compressing to {0}'.format(filename)):
            with tarfile.open(filename, 'w:xz') as txz:
                for name in filenames:
                    txz.add(name)
                # Add translations
                txz.add(translations, 'translations')

    os.chdir(ctx.obj['home'])


@cli.command()
@click.pass_context
@click.option('-p', '--pretty', is_flag=False)
@click.option('-s', '--split', is_flag=True)
@click.option('-c/-nc', '--compress/--no-compress', default=False)
@click.option('-r', '--serialization', default='json',
              type=click.Choice(['json', 'msgpack']))
@click.option('-k', '--keys', default=None)
def full(ctx, drop, pretty, split, compress, serialization, keys):
    '''
    Perfom full processing, execute all operations from download to dist.

    Take more than 3 hours.
    '''
    title(textwrap.dedent(full.__doc__))
    ctx.invoke(download)
    ctx.invoke(preprocess)
    ctx.invoke(load)
    ctx.invoke(aggregate)
    ctx.invoke(postprocess)
    ctx.invoke(dist, pretty=pretty, split=split, compress=compress,
               serialization=serialization, keys=keys)


@cli.command()
@click.pass_context
@click.option('-c/-nc', '--compress/--no-compress', default=False)
def logos(ctx, compress):
    '''Fetch logos from data'''
    title(logos.__doc__)
    zones = ctx.obj['db']
    fetch_logos(zones, DIST_DIR)
    if compress:
        compress_logos(DL_DIR, DIST_DIR)


@cli.command()
@click.pass_context
def status(ctx):
    '''Display some informations and statistics'''
    title('Current status')

    section('Settings')
    click.echo('GEOZONES_HOME: {0}'.format(ctx.obj['home']))
    section('Levels')
    for level in ctx.obj['levels']:
        click.echo('{id}: {label}'.format(**level.__dict__))

    section('downloads')
    filenames = (f for _, f in downloadable_urls(ctx))
    for filename in sorted(filenames):
        click.echo('{0} ... '.format(filename), nl=False)
        if os.path.exists(os.path.join(DL_DIR, filename)):
            success('present')
        else:
            error('missing')

    section('coverage')
    zones = ctx.obj['db']
    total = 0
    properties = ('population', 'area', 'wikipedia', 'geom', 'code')
    totals = dict((prop, 0) for prop in properties)

    def countprop(name):
        results = zones.aggregate([
            {'$match': {
                name: {'$exists': True},
                'level': {'$in': [l.id for l in ctx.obj['levels']]}
            }},
            {'$group': {'_id': '$level', 'value': {'$sum': 1}}}
        ])
        return dict((r['_id'], r['value']) for r in results)

    def display_prop(name, count, total):
        click.echo('\t{0}: '.format(name), nl=False)
        if count == 0:
            func = _secho(fg='red')
        elif count == total:
            func = _secho(fg='green')
        else:
            func = _secho(fg='yellow')
        func('{0}/{1}'.format(count, total))

    counts = dict((p, countprop(p)) for p in properties)
    for level in ctx.obj['levels']:
        count = zones.count({'level': level.id})
        total += count
        msg = '{0}: {1}'.format(level.id, count)
        if count == 0:
            click.secho(msg, fg='yellow')
            continue
        else:
            click.echo(msg)
        for prop in properties:
            prop_count = counts[prop].get(level.id, 0)
            totals[prop] += prop_count
            display_prop(prop, prop_count, count)
    click.secho('TOTAL: {0}'.format(total), bold=True)
    for prop in properties:
        prop_total = totals[prop]
        display_prop(prop, prop_total, total)


@cli.command()
@click.option('-h', '--host', default='localhost', envvar=('HOST', 'GEOZONES_HOST'))
@click.option('-p', '--port', default=5000)
@click.option('-d', '--debug', is_flag=True)
@click.option('-o', '--open', 'launch', is_flag=True)
@click.pass_context
def explore(ctx, host, port, debug, launch):
    '''A web interface to explore data'''
    if not debug:  # Avoid dual title
        title('Running the exploration Web interface')
    from . import explore
    if launch:
        click.launch('http://localhost:5000/')
    explore.run(ctx.obj['db'], host=host, port=port, debug=debug)


if __name__ == '__main__':
    cli()
