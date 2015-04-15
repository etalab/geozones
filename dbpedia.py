import requests
import re

RE_WIKIPEDIA = re.compile(r'https?://(?P<namespace>\w+\.)?wikipedia\.org/wiki/(?P<resource>.+)$')


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
        self.json = None

    def fetch(self):
        url = '{base_url}/data/{resource}.json'.format(**self.__dict__)
        response = requests.get(url)
        data = response.json()
        key = '{base_url}/resource/{resource}'.format(**self.__dict__)
        self.json = data.get(key)
        return self.json

    def __call__(self, *relations):
        if not self.json and not self.fetch():
            return
        for relation in relations:
            data = self.json.get(relation)
            if data:
                return data
