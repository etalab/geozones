#!/usr/bin/env python
import json
import os
import tarfile

from os.path import basename, join, exists
from urllib.request import FancyURLopener, urlopen, Request

import click
from pymongo import MongoClient, ASCENDING

import geojson
from tools import info, success, title, ok, error, section, warning
from geo import root

# Importing levels modules in order (international first)
import international  # noqa
import france  # noqa

DL_DIR = 'downloads'
DIST_DIR = 'dist'
DB_NAME = 'geozones'
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

urlretrieve = FancyURLopener().retrieve


def DB():
    client = MongoClient()
    db = client[DB_NAME]
    collection = db.geozones
    return collection


@click.group(chain=True, context_settings=CONTEXT_SETTINGS)
@click.option('-l', '--level', multiple=True, help='Limits to given levels')
@click.option('-H', '--home', envvar='GEOZONES_HOME',
              help='Specify GeoZones working home')
@click.pass_context
def cli(ctx, level, home):
    if home:
        os.chdir(home)
    else:
        home = os.getcwd()
    ctx.obj['home'] = home

    levels = []
    for l in root.traverse():
        should_process = not level or l.id in level
        if should_process and l not in levels:
            levels.append(l)

    ctx.obj['levels'] = levels


@cli.command()
@click.pass_context
def download(ctx):
    '''Download sources datasets'''
    title('Downloading required datasets')
    if not exists(DL_DIR):
        os.makedirs(DL_DIR)

    urls = (level.urls for level in ctx.obj['levels'] if level.urls)
    urls = set([url for lst in urls for url in lst])
    for url in urls:
        filename = basename(url).strip()

        info('Downloading {0}'.format(filename))

        req = Request(url, method='HEAD')
        req.add_header('Accept-Encoding', 'identity')
        response = urlopen(req)
        size = int(response.headers['Content-Length'])

        with click.progressbar(length=size) as bar:
            def reporthook(blocknum, blocksize, totalsize):
                read = blocknum * blocksize
                if read <= 0:
                    return
                if read > totalsize:
                    bar.update(size)
                else:
                    bar.update(read)

            urlretrieve(url, join(DL_DIR, filename), reporthook=reporthook)


@cli.command()
@click.pass_context
@click.option('-d', '--drop', is_flag=True)
def load(ctx, drop):
    '''Load zones from a folder of zip files containing shapefiles'''
    title('Extracting zones from datasets')
    zones = DB()

    if drop:
        info('Drop existing collection')
        zones.drop()

    with ok('Creating index (level,code)'):
        zones.create_index([('level', ASCENDING), ('code', ASCENDING)])
    info('Creating index (level,keys)')
    zones.create_index([('level', ASCENDING), ('keys', ASCENDING)])
    info('Creating index (parents)')
    zones.create_index('parents')

    total = 0

    for level in ctx.obj['levels']:
        info('Processing level "{0}"'.format(level.id))
        total += level.load(DL_DIR, zones)

    success('Done: Loaded {0} zones'.format(total))


@cli.command()
@click.pass_context
def aggregate(ctx):
    '''Perform zones aggregations'''
    title('Performing zones aggregations')
    zones = DB()

    total = 0

    for level in reversed(ctx.obj['levels']):
        total += level.build_aggregates(zones)

    success('Done: Built {0} zones by aggregation'.format(total))


@cli.command()
@click.pass_context
def postprocess(ctx):
    '''Perform some postprocessing'''
    title('Performing post-processing')
    zones = DB()

    for level in ctx.obj['levels']:
        level.postprocess(DL_DIR, zones)

    success('Post-processing done')


@cli.command()
@click.pass_context
@click.option('-p', '--pretty', is_flag=True)
@click.option('-s', '--split', is_flag=True)
@click.option('-c/-nc', '--compress/--no-compress', default=True)
def dist(ctx, pretty, split, compress):
    '''Dump a distributable GeoJSON file'''
    title('Dumping data to GeoJSON')
    zones = DB()
    filenames = []

    if not exists(DIST_DIR):
        os.makedirs(DIST_DIR)

    os.chdir(DIST_DIR)

    if split:
        for level in ctx.obj['levels']:
            filename = 'zones-{0}.json'.format(level.id.replace('/', '-'))
            with ok('Generating {0}'.format(filename)):
                with open(filename, 'w') as out:
                    data = zones.find({'level': level.id})
                    geojson.dump(data, out, pretty=pretty)
            filenames.append(filename)
    else:
        with ok('Generating zones.json'):
            with open('zones.json', 'w') as out:
                levels = [l.id for l in ctx.obj['levels']]
                data = zones.find({'level': {'$in': levels}})
                geojson.dump(data, out, pretty=pretty)
        filenames.append('zones.json')

    with ok('Generating levels.json'):
        with open('levels.json', 'w') as out:
            data = []
            for level in ctx.obj['levels']:
                data.append({
                    'id': level.id,
                    'label': level.label,
                    'parents': [p.id for p in level.parents]
                })
            if pretty:
                json.dump(data, out, indent=4)
            else:
                json.dump(data, out)
            filenames.append('levels.json')

    if compress:
        filename = 'geozones-translations.tar.xz'
        with ok('Compressing to {0}'.format(filename)):
            with tarfile.open(filename, 'w:xz') as txz:
                txz.add(join(ctx.obj['home'], 'translations'), 'translations')

        filename = 'geozones-split.tar.xz' if split else 'geozones.tar.xz'
        with ok('Compressing to {0}'.format(filename)):
            with tarfile.open(filename, 'w:xz') as txz:
                for name in filenames:
                    txz.add(name)
                # Add translations
                txz.add(join(ctx.obj['home'], 'translations'), 'translations')

    os.chdir(ctx.obj['home'])


@cli.command()
@click.pass_context
@click.option('-d', '--drop', is_flag=True)
@click.option('-p', '--pretty', is_flag=True)
@click.option('-s', '--split', is_flag=True)
@click.option('-c/-nc', '--compress/--no-compress', default=False)
def full(ctx, drop, pretty, split, compress):
    '''
    Perfom a full processing

    Execute all operations from download to dist
    '''
    ctx.invoke(download)
    ctx.invoke(load, drop=drop)
    ctx.invoke(aggregate)
    ctx.invoke(postprocess)
    ctx.invoke(dist, pretty=pretty, split=split, compress=compress)


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
    urls = (level.urls for level in ctx.obj['levels'] if level.urls)
    urls = set([url for lst in urls for url in lst])
    for url in urls:
        filename = basename(url).strip()
        click.echo('{0} ... '.format(filename), nl=False)
        if os.path.exists(os.path.join(DL_DIR, filename)):
            success('present')
        else:
            error('absent')

    section('coverage')
    zones = DB()
    total = 0
    properties = ('population', 'area', 'wikipedia')
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
            func = error
        elif count == total:
            func = success
        else:
            func = warning
        func('{0}/{1}'.format(count, total))

    counts = dict((p, countprop(p)) for p in properties)
    for level in ctx.obj['levels']:
        count = zones.count({'level': level.id})
        total += count
        click.echo('{0}: {1}'.format(level.id, count))

        for prop in properties:
            prop_count = counts[prop].get(level.id, 0)
            totals[prop] += prop_count
            display_prop(prop, prop_count, count)
    click.secho('TOTAL: {0}'.format(total), bold=True)
    for prop in properties:
        prop_total = totals[prop]
        display_prop(prop, prop_total, total)


@cli.command()
@click.option('-d', '--debug', is_flag=True)
@click.option('-o', '--open', 'launch', is_flag=True)
def explore(debug, launch):
    '''A web interface to explore data'''
    if not debug:  # Avoid dual title
        title('Running the exploration Web interface')
    import explore
    if launch:
        click.launch('http://localhost:5000/')
    explore.run(debug)


if __name__ == '__main__':
    cli(obj={})
