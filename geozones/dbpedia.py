import re
import json

import requests

from .tools import error


RDF_PREFIXES = {
    'o:': 'http://dbpedia.org/ontology/',
    'fr:': 'http://fr.dbpedia.org/property/',
}

SPARQL_PREFIXES = '\n'.join(map(
    lambda kv: 'PREFIX {0[0]} <{0[1]}>'.format(kv),
    RDF_PREFIXES.items()
))

# Sparql query for bulk properties retrieval
# given a list of DBPedia URIs
SPARQL_BY_URI = '''{prefixes}
SELECT ?uri ?property ?value WHERE {{
    VALUES ?uri {{ {uris} }}
    VALUES ?property {{ {properties} }}
    ?uri ?property ?value.
}}
'''

RE_WIKIPEDIA = re.compile(
    r'https?://(?P<namespace>\w+)?\.?wikipedia\.org/wiki/(?P<path>.+)$')

SPARQL_SERVER = 'http://dbpedia.inria.fr/sparql'


def extract_uri(uri):
    '''Extract a DBPedia URI from a Wikipedia identifier or URL'''
    if not uri:
        return
    uri = uri.strip().replace(' ', '_')
    # Special wrong case: `fr:fr:Communauté_de_communes_d'Altkirch`
    if uri.startswith('fr:fr:'):
        namespace, _, path = uri.split(':')
    elif ':' in uri and not uri.startswith('http'):
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


def execute_sparql_query(query, graph='http://fr.dbpedia.org'):
    '''
    Execute a SPARQL query and returns a list of n-uplets.
    '''
    headers = {
        'User-Agent': 'geozones/1.0 (http://github.com/etalab/geozones)',
    }
    parameters = {
        'default-graph-uri': graph,
        'query': query,
        'format': 'json'
    }
    try:
        response = requests.post(SPARQL_SERVER,
                                 data=parameters,
                                 headers=headers)
    except requests.exceptions.ReadTimeout:
        error('Timeout:', SPARQL_SERVER, parameters)
        return []
    try:
        data = response.json()
    except json.decoder.JSONDecodeError:
        error('JSON Error: {0} {1} {2}',
              SPARQL_SERVER, parameters, response.text)
        return []
    return data['results']['bindings']
