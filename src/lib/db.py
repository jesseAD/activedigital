import pymongo


# create a class for mongodb connection
class MongoDB:
    def __init__(self, db_name, collection_name="positions", uri=None):
        self.client = pymongo.MongoClient(uri)

        self.db = self.client[db_name]

        self.collection = self.db[collection_name]

    def insert(self, data):
        self.collection.insert_one(data)

    def find(self, query):
        return self.collection.find(query)

    def find_one(self, query):
        return self.collection.find_one(query)

    def update(self, query, data, upsert = False):
        self.collection.update_one(query, {"$set": data}, upsert=upsert)

    def delete(self, query):
        self.collection.delete_one(query)

    def delete_many(self, query):
        self.collection.delete_many(query)

    def count(self, query):
        return self.collection.count_documents(query)

    def close(self):
        self.client.close()

    def aggregate(self, query):
        results = []

        cursor = self.collection.aggregate(query)

        for x in cursor:
            results.append(x)

        return results

    def status(self):
        return self.client.server_info()

    def verify(self):
        return self.db_name in self.client.list_database_names()
