from pymongo import MongoClient
import csv
import json
import pandas as pd

class loaddata():
    def __init__(self):
        self.client = self.setup_db_connection()
        self.db = self.client['event-recommendation']
        #self.load_friends() -- done
        #self.load_user_info() -- done
        #self.load_attendance_info()
        #self.load_event_info()
        self.update_attendance_events()
        #self.update_attendance_train_data()

    def setup_db_connection(self):
        print("connecting to mongo db..")
        client = MongoClient('localhost', 27017)
        return client

    def load_friends(self):
        print("loading user friends info to db")
        friends = self.db['friends_info']
        with open('user_friends.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            columns = reader.fieldnames
            print(columns)
            for row in reader:
                record = {
                    'uid': row['user'],
                    'friends': [u.strip() for u in row['friends'].split()]
                }
                friends.insert_one(record)

       

    def load_user_info(self):
        print("loading user info to db")
        user_info = self.db['user_info']
        with open('users.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            columns = reader.fieldnames
            print(columns)
            for row in reader:
                a = row['birthyear']
                try:
                    a = int(a)
                    if a < 1940:
                        a = None
                    else:
                        a = 2013 - a
                except:
                    a = None
                record = {
                    'uid': row['user_id'].strip(),
                    'locale': row['locale'],
                    'birthyear': row['birthyear'],
                    'gender': row['gender'],
                    'joinedAt': row['joinedAt'],
                    'location':row['location'],
                    'timezone':row['timezone'],
                    'age':a
                }
                user_info.insert_one(record)

    def update_attendance(self, uid, eid, att_type1, att_type2):
        attendance_info = self.db['attendance_info']
        attendance_info.update(
            {'uid': uid, 'eid': eid},
            {'$set': {'uid': uid, 'eid': eid, att_type1: True, att_type2: True}}, 
            upsert=True)

    def load_attendance_info(self):
        print("loading attendance info to db")
        event_attendees = pd.read_csv("event_attendees.csv")
        count = 0
        for event in event_attendees.iterrows():
            print(count)
            count = count + 1
            event = event[1]
            eid = event['event']
            for attr in ['yes', 'maybe', 'invited', 'no']:
                users = event[attr]
                if isinstance(users, float):
                    continue
                users = [int(u) for u in users.split()]
                for uid in users:
                    self.update_attendance(uid, eid, attr)
    

    def load_event_info(self):
        print("loading event info to db")
        event_info = self.db['event_info']
        chunks = pd.read_csv("events.csv", iterator=True, chunksize=1000)
        count = 0
        for chunk in chunks:
            print(count)
            count += 1000
            #if count < 1564000:
            #    continue
            for e in chunk.iterrows():
                e = e[1]
                eid = e['event_id']
                # location = None
                # if e['lat'] and e['lng'] and not isnan(e['lat']) and not isnan(e['lng']):
                #     location = [e['lat'], e['lng']]
                # elif e['city'] or e['state'] or e['country']:
                #     loc_string = '%s, %s, %s' % (e['city'], e['state'], e['country'])
                #     if loc_string in location_cache:
                #         location = location_cache[loc_string]
                #     else:
                #         location = get_coordinates_from_text(loc_string)
                #         if location:
                #             location_cache[loc_string] = location
                            
                words = list(e[9:110])
                event = {
                    'id': eid,
                    #'location': location,
                    'words': words
                }
                event_info.insert([event])
        


    def update_attendance_events(self):
        chunks = pd.read_csv("events.csv", iterator=True, chunksize=1000)
        count = 0
        for chunk in chunks:
            print(count)
            count += 1000
            
            for e in chunk.iterrows():
                e = e[1]
                eid = e['event_id']
                uid = int(e['user_id'])
                
                self.update_attendance(uid, eid, 'yes', 'interested')
                #self.update_attendance(uid, eid, 'interested')
    

    def update_attendance_train_data(self):
        train = pd.read_csv( "train.csv", converters={"timestamp": parse})
        for pair in train.iterrows():
            pair = pair[1]
            uid = pair['user']
            eid = pair['event']
            for attr in ['invited', 'interested', 'not_interested']:
                if pair[attr]:
                    self.update_attendance(uid, eid, attr)



if __name__ == "__main__":
    loaddata()




# def load_attendance_info(self):
#     print("loading attendance info to db")
#     attendance_info = self.db['attendance_info']
#     with open('event_attendees.csv',newline='') as csvfile:
#         reader = csv.DictReader(csvfile)
#         columns  =reader.fieldnames
#         print(columns)
#         for row in reader:
#             record = {}
#             for key in columns:
#                 record[key]=row[key]
#             attendance_info.insert_one(record)
