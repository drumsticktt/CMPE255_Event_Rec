from pymongo import MongoClient
import csv
import json

class loaddata():
    def __init__(self):
        self.client = self.setup_db_connection()
        self.db = self.client['event-recommendation']
        #self.load_friends()
        self.load_user_info()




    def setup_db_connection(self):
        print("connecting to mongo db..")
        client = MongoClient('localhost', 27017)
        return client

    def load_friends(self):
        print("loading user friends info to db")
        friends = self.db['friends_info']
        with open('user_friends.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                record = {
                    'uid': row['user'],
                    'friends': row['friends']
                }
                friends.insert_one(record)

    def load_user_info(self):
        print("loading user info to db")
        user_info = self.db['user_info']
        with open('users.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                record = {
                    'uid': row['user_id'],
                    'locale': row['locale'],
                    'birthyear': row['birthyear'],
                    'gender': row['gender'],
                    'joinedAt': row['joinedAt'],
                    'location':row['location'],
                    'timezone':row['timezone']

                }
                user_info.insert_one(record)


    
    # add attendance

    # add event info



           
    

    











if __name__ == "__main__":
    loaddata()