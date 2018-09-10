import click

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

from .tools import error

DB_NAME = 'geozones'


class DB(Collection):

    def __init__(self, url):
        client = MongoClient(url)
        db = client[DB_NAME]
        super().__init__(db, 'geozones')

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

    def fetch_zones(self, level, code=None, before=None, after=None):
        """
        Retrieve zones for a given level and code before/after a date
        including that date.

        The `before` and `after` dates must be strings like `YYYY-MM-DD`.
        They are mutually exclusive.
        """
        if before:
            end = {'$lte': before}
        elif after:
            end = {'$gte': after}
        else:
            raise ValueError('You must set the "before" or "after" parameters')
        conditions = {
            'level': level,
            'validity.end': end,
        }
        if code:
            conditions['code'] = code
        return self.find(conditions)

    def fetch_zone(self, level, code=None, before=None, after=None):
        """
        Retrieve the latest zone for a given level and code before/after a date
        including that date.

        The `before` and `after` dates must be a strings like `YYYY-MM-DD`.
        They are mutually exclusive.
        """
        zone = list(self.fetch_zones(level, code, before, after)
                        .sort('-validity.start')
                        .limit(1))
        return zone and zone[0] or None

    def aggregate_with_progress(self, pipeline):
        '''
        Iter over the result of an aggregation and display a progress bar.

        A first aggregation is done to count the total number of items.
        '''
        count_result = next(self.aggregate(pipeline + [
            {'$group': {'_id': None, 'count': {'$sum': 1}}}
        ]))
        total = int(count_result['count'])

        with click.progressbar(self.aggregate(pipeline),
                               width=0, length=total) as bar:
            for item in bar:
                yield item
