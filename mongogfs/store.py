# -*- coding=utf -*-
from ...stores import Store
import pymongo

__all__ = []

class MongogfsStore(Store):
    def __init__(self, url, database=None, collection=None, bucket=None, **options):

        super(MongogfsStore, self).__init__(**options)

        # self.client = pymongo.MongoClient(url, read_preference=pymongo.read_preferences.ReadPreference.SECONDARY)
        self.client = pymongo.MongoClient(url)
        self.database = database
        # self.fs = gridfs.GridFS()
        self.collection = collection
        self.bucket = bucket
        self.options = options
        self.store_type = options['store_type']
        # self.filesystem = gridfs.GridFS(database)
