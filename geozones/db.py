from datetime import date

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

from .tools import error, progress

DB_NAME = 'geozones'
TODAY = date.today().isoformat()


class DB(Collection):
    TODAY = TODAY

    def __init__(self, url):
        client = MongoClient(url)
        db = client[DB_NAME]
        super().__init__(db, 'geozones')

    def initialize(self):
        '''Initialize indexes'''
        # If index already exists it will not be recreated
        self.create_index([('level', ASCENDING), ('code', ASCENDING)])
        self.create_index([('level', ASCENDING), ('keys', ASCENDING)])
        self.create_index('parents')

    def safe_bulk_insert(self, data):
        '''
        Try to insert in bulk and returns the number of insertions.
        '''
        try:
            result = self.insert_many(data)
            return len(result.inserted_ids)
        except BulkWriteError as e:
            messages = '\n\t'.join(
                # brackets in errors needs to be escaped because of
                # the  underlying `.format()` call in `error()`
                err['errmsg'].replace('{', '{{').replace('}', '}}')
                for err in e._OperationFailure__details['writeErrors']
            )
            error(':\n\t'.join((str(e), messages)))

            return e._OperationFailure__details['nInserted']

    def _valid_at(self, at=None):
        '''Build a validity query for a given date'''
        if at is None:
            return {}
        if isinstance(at, date):
            at = date.isoformat()
        return {'$or': [
            # Zones without validity boundings, ie. valid anytime
            {'validity': None},
            {'validity.start': None, 'validity.end': None},
            # Ended zones with matching validity boundings
            {'validity.start': {'$lte': at}, 'validity.end': {'$gt': at}},
            # Not ended zones with matching validity start bounding
            {'validity.start': {'$lte': at}, 'validity.end': None},
            # Ended zones with undefined start and matching validity end bounding
            {'validity.start': None, 'validity.end': {'$gt': at}},
        ]}

    def zone(self, level, code, at=None, **kwargs):
        '''Get a Zone given its level, its code and a date'''
        query = self._valid_at(at)
        query.update(level=level, code=code, **kwargs)
        return self.find_one(query)

    def update_zone(self, level, code, at=None, ops=None):
        '''Update a Zone given its level, its code and a date'''
        query = self._valid_at(at)
        query.update(level=level, code=code)
        return self.find_one_and_update(query, ops)

    def update_zones(self, level, code, at=None, ops=None):
        '''Update a Zone given its level, its code and a date'''
        query = self._valid_at(at)
        query.update(level=level, code=code)
        return self.update_many(query, ops)

    def level(self, level, at=None, **kwargs):
        '''Get all Zones for a given level and a date'''
        query = self._valid_at(at)
        query.update(level=level, **kwargs)
        return self.find(query)

    def aggregate_with_progress(self, pipeline, msg=None):
        '''
        Iter over the result of an aggregation and display a progress bar.

        A first aggregation is done to count the total number of items.
        '''
        count_result = next(self.aggregate(pipeline + [
            {'$group': {'_id': None, 'count': {'$sum': 1}}}
        ]))
        total = int(count_result['count'])

        for item in progress(self.aggregate(pipeline), msg=msg, length=total):
            yield item
