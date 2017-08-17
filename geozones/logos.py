import os
import tarfile

import requests

from .tools import info, success

DBPEDIA_MEDIA_URL = 'http://commons.wikimedia.org/wiki/Special:FilePath/'
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
    if not os.path.exists(path):
        os.makedirs(path)
    for zone in zones.find({'$or': [
            {'flag': {'$exists':  True}},
            {'blazon': {'$exists':  True}},
            {'logo': {'$exists':  True}}
            ]},
            no_cursor_timeout=True):
        filename = zone.get('flag', zone.get('blazon', zone.get('logo')))
        if not filename:
            continue
        filepath = os.path.join(path, filename)
        if os.path.exists(filepath):
            continue
        url = DBPEDIA_MEDIA_URL + filename
        print('Fetching {url}'.format(url=url))
        r = requests.get(url, stream=True, headers=HEADERS)
        if r.status_code == 404:
            continue
        with open(filepath, 'wb') as file_destination:
            for chunk in r.iter_content(chunk_size=1024):
                file_destination.write(chunk)
    success('Logos fetched')


def compress_logos(dl_dir, dist_dir):
    """Compress the `logos` folders into a unique archive file."""
    filename = os.path.join(dist_dir, LOGOS_FILENAME)
    path = os.path.join(dl_dir, LOGOS_FOLDER_PATH)
    info('Compressing logos to {filename}', filename=filename)
    with tarfile.open(filename, 'w:xz') as txz:
        for (dirpath, dirnames, filenames) in os.walk(path):
            for name in filenames:
                txz.add(os.path.join(path, name))
            break
    success('Compressing done')
