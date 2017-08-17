from pymongo import MongoClient

DB_NAME = 'geozones'


def DB():
    client = MongoClient()
    db = client[DB_NAME]
    collection = db.geozones
    return collection
