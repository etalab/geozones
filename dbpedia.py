import requests
import re
from string import Template


RE_WIKIPEDIA = re.compile(r'https?://(?P<namespace>\w+\.)?wikipedia\.org/wiki/(?P<resource>.+)$')
DBPEDIA_POPULATION = (
    'http://fr.dbpedia.org/property/population',
    'http://dbpedia.org/ontology/populationTotal',
)

DBPEDIA_AREA = (
    'http://fr.dbpedia.org/property/superficie',
    'http://dbpedia.org/ontology/area'
)
DBPEDIA_ONTOLOGIES = {
    'population': DBPEDIA_POPULATION,
    'area': DBPEDIA_AREA,
}
SPARQL_SERVER = 'http://dbpedia.inria.fr/sparql'
SPARQL_TEMPLATE = Template('select * where {<$resource> <$ontology> ?$name}')


class DBPedia(object):

    def __init__(self, resource):
        resource = resource.strip('').replace(' ', '_')
        if ':' in resource:
            namespace, self.resource = resource.split(':')
        elif RE_WIKIPEDIA.match(resource):
            m = RE_WIKIPEDIA.match(resource)
            namespace, self.resource = m.group('namespace', 'resource')
        else:
            self.resource = resource
            namespace = None

        if namespace:
            self.base_url = 'http://{0}.dbpedia.org'.format(namespace)
        else:
            self.base_url = 'http://dbpedia.org'

    def fetch(self, name):
        resource = '{base_url}/resource/{resource}'.format(
            base_url=self.base_url, resource=self.resource)

        # First try the French attribute.
        sparql_query = SPARQL_TEMPLATE.substitute(
            resource=resource,
            ontology=DBPEDIA_ONTOLOGIES[name][0],
            name=name)
        parameters = {
            'default-graph-uri': 'http://fr.dbpedia.org',
            'query': sparql_query,
            'format': 'json'
        }
        response = requests.get(SPARQL_SERVER, params=parameters)
        data = response.json()
        try:
            return data['results']['bindings'][0][name]['value']
        except IndexError:
            pass

        # Then fallback on the international one.
        parameters['query'] = SPARQL_TEMPLATE.substitute(
            resource=resource,
            ontology=DBPEDIA_ONTOLOGIES[name][1],
            name=name)
        response = requests.get(SPARQL_SERVER, params=parameters)
        data = response.json()
        try:
            return data['results']['bindings'][0][name]['value']
        except IndexError:
            return
