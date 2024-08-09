from pymongo import MongoClient
from datetime import datetime, timedelta


CONNECTION_STRING = "mongodb+.bojn1so.mongodb.net/"
client = MongoClient(CONNECTION_STRING)
mydatabase = client['copy_last_id'] 
mycollection = mydatabase['copy_ubuy_last_id1'] 

start_date = datetime.now() 

admin_db = client.admin
server_status = admin_db.command("serverStatus")
opcounters = server_status["opcounters"]
print(opcounters)
