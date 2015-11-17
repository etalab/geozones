import requests
import re
from string import Template


RE_WIKIPEDIA = re.compile(
    r'https?://(?P<namespace>\w+\.)?wikipedia\.org/wiki/(?P<resource>.+)$')
SPARQL_SERVER = 'http://dbpedia.inria.fr/sparql'
# We want population and/or area from French DBPedia or
# their international counterparts as fallbacks.
SPARQL_TEMPLATE = Template('''SELECT ?population ?area WHERE {
    {<$resource> <http://fr.dbpedia.org/property/population>|
                 <http://dbpedia.org/ontology/populationTotal> ?population}
UNION
    {<$resource> <http://fr.dbpedia.org/property/superficie>|
                 <http://dbpedia.org/ontology/area> ?area}
}''')


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

    def fetch_population_and_area(self):
        resource = '{base_url}/resource/{resource}'.format(
            base_url=self.base_url, resource=self.resource)

        sparql_query = SPARQL_TEMPLATE.substitute(resource=resource)
        parameters = {
            'default-graph-uri': 'http://fr.dbpedia.org',
            'query': sparql_query,
            'format': 'json'
        }
        response = requests.get(SPARQL_SERVER, params=parameters)
        data = response.json()
        try:
            results = data['results']['bindings'][0]
        except IndexError:
            return
        population_and_area = {}
        if 'population' in results:
            population_and_area['population'] = results['population']['value']
        if 'area' in results:
            population_and_area['area'] = results['area']['value']
        return population_and_area
