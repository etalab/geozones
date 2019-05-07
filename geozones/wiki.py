'''
WikiPedia/Data helpers
'''
import re
import json
import itertools

import requests

from .tools import error

RE_WIKIPEDIA = re.compile(r'https?://(?P<namespace>\w+)?\.?wikipedia\.org/wiki/(?P<path>.+)$')
RE_DBPEDIA = re.compile(r'https?://(?P<namespace>\w+)?\.?dbpedia\.org/resource/(?P<path>.+)$')
RE_MEDIA_COMMONS = re.compile(r'https?://commons\.wikimedia\.org/wiki/Special:FilePath/(?P<path>.+)$')

WIKIDATA_SPARQL = 'https://query.wikidata.org/sparql'
WD = 'http://www.wikidata.org/entity/'


def wikipedia_to_dbpedia(uri):
    '''Extract a DBPedia URI from a Wikipedia identifier or URL'''
    if not uri:
        return
    uri = uri.strip().replace(' ', '_')
    # Special wrong case: `fr:fr:Communauté_de_communes_d'Altkirch`
    if uri.startswith('fr:fr:'):
        namespace, _, path = uri.split(':')
    elif ':' in uri and not uri.startswith('http:') and not uri.startswith('https:'):
        namespace, path = uri.split(':')
    elif RE_WIKIPEDIA.match(uri):
        m = RE_WIKIPEDIA.match(uri)
        namespace, path = m.group('namespace', 'path')
    else:
        path = uri
        namespace = None

    if namespace:
        base_url = 'http://{0}.dbpedia.org'.format(namespace)
    else:
        base_url = 'http://dbpedia.org'
    return '{base_url}/resource/{path}'.format(base_url=base_url, path=path)


def wikipedia_url_to_id(url):
    if not url:
        return
    if ':' in url and not url.startswith('http:') and not url.startswith('https:'):
        return url
    m = RE_WIKIPEDIA.match(url)
    namespace, path = m.group('namespace', 'path') if m else (None, url)
    return ':'.join((namespace, path)) if namespace else path


def dbpedia_to_wikipedia(uri):
    '''Get the wikipedia identifier from a DBPedia URI'''
    if not uri:
        return
    m = RE_DBPEDIA.match(uri)
    if not m:
        return
    namespace, path = m.group('namespace', 'path')
    if namespace:
        return ':'.join((namespace, path))
    else:
        return path


def media_url_to_path(url):
    '''Extract path from a wikimedia commons URL'''
    if not url:
        return
    return RE_MEDIA_COMMONS.sub('\g<path>', url)


def data_uri_to_id(uri):
    return uri.replace(WD, '') if uri else None


def data_sparql_query(query, graph='http://fr.dbpedia.org'):
    '''
    Execute a SPARQL query and returns a list of n-uplets.
    '''
    headers = {
        'User-Agent': 'geozones/1.0 (http://github.com/etalab/geozones)',
        'Accept': 'application/sparql-results+json',
    }
    parameters = {
        # 'default-graph-uri': graph,
        'query': query,
        'format': 'json'
    }

    try:
        response = requests.post(WIKIDATA_SPARQL,
                                 data=parameters,
                                 headers=headers)
    except requests.exceptions.ReadTimeout:
        error('Timeout:', WIKIDATA_SPARQL, parameters)
        return []
    try:
        data = response.json()
    except json.decoder.JSONDecodeError:
        error('JSON Error: {0} {1} {2}',
              WIKIDATA_SPARQL, parameters, response.text)
        return []
    # print('data', data_reduce_result())
    return data['results']['bindings']


def data_reduce_result(data, key, *aggs):
    out = []
    data = [{k: v['value'] for k, v in row.items()} for row in data]

    for id, grp in itertools.groupby(data, lambda r: r[key]):
        item = {agg: set() for agg in aggs}
        item[key] = id
        for row in grp:
            for k, v in row.items():
                if k in aggs:
                    item[k].add(v)
                else:
                    item[k] = v
        for agg in aggs:
            item[agg] = list(item[agg])
        out.append(item)
    return out
