import os
import tarfile

import click
import requests

from .tools import info, success, unicodify

DBPEDIA_MEDIA_URL = 'https://commons.wikimedia.org/wiki/Special:FilePath/'
LOGOS_FOLDER_PATH = 'logos'
LOGOS_FILENAME = 'geologos.tar.xz'

# Define a user agent to follow https://www.mediawiki.org/wiki/API:Etiquette
USER_AGENT = 'geozones/1.0 (https://github.com/etalab/geozones)'
HEADERS = {'user-agent': USER_AGENT}


def fetch_logos(zones, dl_dir):
    """
    Fetch logos (logos or flags or blazons) from `zones`.

    Not optimized to avoid being blacklisted by wikimedia.
    That command takes about 3h30 as of February 2016.

    Existing files are not fetched but previous 404 are retried.
    """
    info('Fetching logos from Wikimedia')
    path = os.path.join(dl_dir, LOGOS_FOLDER_PATH)
    logfile_path = os.path.join(dl_dir, 'logos.log')
    logfile = open(logfile_path, 'w')
    if not os.path.exists(path):
        os.makedirs(path)
    downloaded = 0
    query = {'$or': [
        {'flag': {'$exists':  True}},
        {'blazon': {'$exists':  True}},
        {'logo': {'$exists':  True}}
    ]}
    count = zones.count(query)
    cursor = zones.find(query, no_cursor_timeout=True)
    with click.progressbar(cursor, width=0, length=count) as bar:
        for zone in bar:
            filename = zone.get('flag', zone.get('blazon', zone.get('logo')))
            if not filename:
                continue
            filepath = os.path.join(path, filename)
            if os.path.exists(filepath):
                logfile.write('{0} : skipped\n'.format(filename))
                logfile.flush()
                continue
            url = DBPEDIA_MEDIA_URL + unicodify(filename)
            r = requests.get(url, stream=True, headers=HEADERS)
            if r.status_code != 200:
                logfile.write('{0} : {1}\n'.format(url, r.status_code))
                logfile.flush()
                continue
            with open(filepath, 'wb') as file_destination:
                for chunk in r.iter_content(chunk_size=1024):
                    file_destination.write(chunk)
                downloaded += 1
    logfile.close()
    success('{0} logos fetched for {1} candidates', downloaded, count)


def compress_logos(dl_dir, dist_dir):
    """Compress the `logos` folders into a unique archive file."""
    filename = os.path.join(dist_dir, LOGOS_FILENAME)
    path = os.path.join(dl_dir, LOGOS_FOLDER_PATH)
    info('Compressing logos to {filename}', filename=filename)
    with tarfile.open(filename, 'w:xz') as txz:
        txz.add(path, 'logos')
    success('Compressing done')
