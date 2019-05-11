import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import *  # NOQA: Syntaxic sugar to allows "except http.Timeout" like syntax
from urllib3.util.retry import Retry


def get(url, params=None, retry=3, **kwargs):
    '''Wraps requests.get to handle retries'''
    session = _with_retries(retry)
    return session.get(url, params=params, **kwargs)


def post(url, data=None, json=None, retry=3, **kwargs):
    '''Wraps requests.post to handle retries'''
    session = _with_retries(retry)
    return session.post(url, data=data, json=json, **kwargs)


def _with_retries(retry=3):
    session = requests.Session()
    retries = Retry(total=retry, backoff_factor=0.2,
                    status_forcelist=[500, 502, 503, 504],
                    method_whitelist=frozenset(['GET', 'POST'])
                    )
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session
