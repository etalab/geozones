from ..db import TODAY
from ..model import Level, country, country_subset

_ = lambda s: s  # noqa: E731

region = Level('fr:region', _('French region'), 40, country)
epci = Level('fr:epci', _('French intermunicipal (EPCI)'), 68, country)
departement = Level('fr:departement', _('French county'), 60, region)
collectivite = Level('fr:collectivite', _('French overseas collectivities'),
                     60, region)
arrondissement = Level('fr:arrondissement', _('French arrondissement'), 70, departement)
commune = Level('fr:commune', _('French town'), 80, arrondissement, epci)
canton = Level('fr:canton', _('French canton'), 98, departement)
iris = Level('fr:iris', _('Iris (Insee districts)'), 98, commune)

COMMUNES_START = '1943-01-01'

# Cities with districts
PARIS_DISTRICTS = ['fr:commune:751{0:0>2}@{1}'.format(i, COMMUNES_START) for i in range(1, 21)]
MARSEILLE_DISTRICTS = ['fr:commune:132{0:0>2}@{1}'.format(i, COMMUNES_START) for i in range(1, 17)]
LYON_DISTRICTS = ['fr:commune:6938{0}@{1}'.format(i, COMMUNES_START) for i in range(1, 9)]

CONTOURS_ETALAB = ('http://etalab-datasets.geo.data.gouv.fr/contours-administratifs/'
                   '{year}/geojson/{level}-{precision}.geojson.gz')

WIKIDATA_FLAG_OF_FRANCE = 'http://commons.wikimedia.org/wiki/Special:FilePath/Flag%20of%20France.svg'


def contours_etalab(year, level, precision):
    return CONTOURS_ETALAB.format(**locals())


def departements_metropole(db, at=TODAY):
    return db.level(departement.id, at, code={'$regex': '^[0-9a-z]{2}$'})


def droms(db, at=TODAY):
    return db.level(departement.id, at, code={'$regex': '^[0-9a-z]{3}$'})


country_subset.aggregate(
    'fr:metro', _('Metropolitan France'),
    lambda db: [zone['_id'] for zone in departements_metropole(db)],
    parents=['country:fr', 'country-group:ue', 'country-group:world'],
    wikipedia='fr:France_métropolitaine',
    wikidata='Q212429'
)

country_subset.aggregate(
    'fr:drom', 'DROM',
    lambda db: [zone['_id'] for zone in droms(db)],
    parents=['country:fr', 'country-group:ue', 'country-group:world'],
    wikipedia='fr:D%C3%A9partement_et_r%C3%A9gion_d%27outre-mer',
    wikidata='Q202216'
)

country_subset.aggregate(
    'fr:dromcom', 'DROM-COM',
    lambda db: (
        [zone['_id'] for zone in droms(db)] +
        [zone['_id'] for zone in db.level(collectivite.id, TODAY)]
    ),
    parents=['country:fr', 'country-group:ue', 'country-group:world'],
    wikipedia='fr:France_d%27outre-mer',
    wikidata='Q203396'
)
