from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from .tools import error

DB_NAME = 'geozones'


def DB():
    client = MongoClient()
    db = client[DB_NAME]
    collection = db.geozones
    return collection


def safe_bulk_insert(collection, data):
    '''
    Try to insert in bulk and returns the number of insertions.
    '''
    try:
        result = collection.insert_many(data)
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
