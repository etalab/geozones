from ..db import DB
from ..model import Level, country, country_subset

from .histo import (
    retrieve_current_metro_departements, retrieve_current_drom_departements,
    retrieve_current_collectivites
)

_ = lambda s: s  # noqa: E731

region = Level('fr:region', _('French region'), 40, country)
epci = Level('fr:epci', _('French intermunicipal (EPCI)'), 68, country)
departement = Level('fr:departement', _('French county'), 60, region)
collectivite = Level('fr:collectivite', _('French overseas collectivities'),
                     60, region)
arrondissemnt = Level('fr:arrondissement', _('French arrondissemnt'), 70, departement)
commune = Level('fr:commune', _('French town'), 80, arrondissemnt, epci)
canton = Level('fr:canton', _('French canton'), 98, departement)

# Not opendata yet
iris = Level('fr:iris', _('Iris (Insee districts)'), 98, commune)

# Cities with districts
PARIS_DISTRICTS = [
    'fr:commune:751{0:0>2}@1942-01-01'.format(i) for i in range(1, 21)]

MARSEILLE_DISTRICTS = [
    'fr:commune:132{0:0>2}@1942-01-01'.format(i) for i in range(1, 17)]

LYON_DISTRICTS = [
    'fr:commune:6938{0}@1942-01-01'.format(i) for i in range(1, 9)]


country_subset.aggregate(
    'fr:metro', _('Metropolitan France'),
    [zone['_id'] for zone in retrieve_current_metro_departements(DB())],
    parents=['country:fr', 'country-group:ue', 'country-group:world'])

country_subset.aggregate(
    'fr:drom', 'DROM',
    [zone['_id'] for zone in retrieve_current_drom_departements(DB())],
    parents=['country:fr', 'country-group:ue', 'country-group:world'])

country_subset.aggregate(
    'fr:dromcom', 'DROM-COM',
    [zone['_id'] for zone in retrieve_current_drom_departements(DB())] +
    [zone['_id'] for zone in retrieve_current_collectivites(DB())],
    parents=['country:fr', 'country-group:ue', 'country-group:world'])
