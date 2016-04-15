import re
import json
from string import Template

import requests

from tools import warning


RE_WIKIPEDIA = re.compile(
    r'https?://(?P<namespace>\w+)?\.?wikipedia\.org/wiki/(?P<resource>.+)$')
SPARQL_SERVER = 'http://dbpedia.inria.fr/sparql'
# We want population and/or area from French DBPedia or
# their international counterparts as fallbacks.
SPARQL_POPULATION_TEMPLATE = Template('''SELECT ?population ?area WHERE {
    {<$resource_url> <http://fr.dbpedia.org/property/population>|
                     <http://dbpedia.org/ontology/populationTotal> ?population}
UNION
    {<$resource_url> <http://fr.dbpedia.org/property/superficie>|
                     <http://dbpedia.org/ontology/area> ?area}
}''')

# We want flag and/or blason from French DBPedia or
# their international counterparts as fallbacks.
SPARQL_IMAGE_TEMPLATE = Template('''SELECT ?flag ?blazon WHERE {
    {<$resource_url> <http://dbpedia.org/ontology/flag> ?flag}
UNION
    {<$resource_url> <http://dbpedia.org/ontology/blazon> ?blazon}
}''')


class DBPedia(object):

    def __init__(self, resource):
        resource = resource.strip('').replace(' ', '_')
        # Special wrong case: `fr:fr:Communauté_de_communes_d'Altkirch`
        if resource.startswith('fr:fr:'):
            namespace, _, self.resource = resource.split(':')
        elif ':' in resource and not resource.startswith('http'):
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
        self.resource_url = '{base_url}/resource/{resource}'.format(
            base_url=self.base_url, resource=self.resource)

    def fetch_population_or_area(self):
        sparql_query = SPARQL_POPULATION_TEMPLATE.substitute(
            resource_url=self.resource_url)
        parameters = {
            'default-graph-uri': 'http://fr.dbpedia.org',
            'query': sparql_query,
            'format': 'json'
        }
        result = {}
        try:
            response = requests.get(SPARQL_SERVER, params=parameters)
        except requests.exceptions.ReadTimeout:
            warning('Timeout:', SPARQL_SERVER, parameters)
            return result
        try:
            data = response.json()
        except json.decoder.JSONDecodeError:
            warning('JSON Error:', SPARQL_SERVER, parameters, response.text)
            return result
        try:
            results = data['results']['bindings'][0]
        except IndexError:
            return result
        if 'population' in results:
            result['population'] = int(results['population']['value'])
        if 'area' in results:
            result['area'] = int(round(float(results['area']['value'])))
        return result

    def fetch_flag_or_blazon(self):
        sparql_query = SPARQL_IMAGE_TEMPLATE.substitute(
            resource_url=self.resource_url)
        parameters = {
            'default-graph-uri': 'http://fr.dbpedia.org',
            'query': sparql_query,
            'format': 'json'
        }
        flag_or_blazon = {}
        try:
            response = requests.get(SPARQL_SERVER, params=parameters)
        except requests.exceptions.ReadTimeout:
            warning('Timeout:', SPARQL_SERVER, parameters)
            return flag_or_blazon
        try:
            data = response.json()
        except json.decoder.JSONDecodeError:
            warning('JSON Error:', SPARQL_SERVER, parameters, response.text)
            return flag_or_blazon
        try:
            results = data['results']['bindings'][0]
        except IndexError:
            return flag_or_blazon
        if 'flag' in results:
            flag_name = results['flag']['value'].replace(' ', '_')
            flag_or_blazon['flag'] = flag_name
        if 'blazon' in results:
            blazon_name = results['blazon']['value'].replace(' ', '_')
            flag_or_blazon['blazon'] = blazon_name
        return flag_or_blazon
