from pymongo import MongoClient

class Connection_mongo_Db:
    def __init__(self):
        self.CONNECTION_STRING = "mongodbo.mongodb.net/"
        self.database_name = 'copy_last_id'
        self.collection_name = 'copy_ubuy_last_id2'


    def get_connection(self):
        self.client = MongoClient(self.CONNECTION_STRING)
        self.mydatabase = self.client[self.database_name] 
        self.mycollection = self.mydatabase[self.collection_name]
        return self.mycollection
    
    
    def get_connection_close(self):
        self.client.close()


    def insert_or_get_last_id(self, cols , last_id_ = 1):
        if cols is not None:
            data = cols.find_one({"index_name": 'last_id_index_name'})
            if data is None:
                cols.insert_one({"index_name": "last_id_index_name", "last_id": last_id_})
                data = cols.find_one({"index_name": 'last_id_index_name'})
                last_id = data["last_id"]
            else:
                last_id = data["last_id"]
        return last_id


    def update_last_id(self, cols, last_id):
        if cols is not None:
            data = cols.update_one({"index_name": "last_id_index_name"}, {'$set': {"last_id": last_id}})
            print("***** Last_id updated ******")