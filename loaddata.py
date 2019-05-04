from pymongo import MongoClient
import csv
import json
import pandas as pd
import numpy as np
import time
from dateutil.parser import parse

class loaddata():
    def __init__(self):
        self.client = self.setup_db_connection()
        self.db = self.client['event-recommendation']
        self.attendance = self.db['attendance_info']
        self.user_info = self.db['user_info']
        #self.load_friends()
        #self.load_user_info()
        self.load_attendance_info_2()
        #self.load_event_info_2()

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

    '''
    Populate events dictionary
    '''
    def load_event_info_2(self):
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
                '''
                location = None
                if e['lat'] and e['lng'] and not isnan(e['lat']) and not isnan(e['lng']):
                    location = [e['lat'], e['lng']]
                elif e['city'] or e['state'] or e['country']:
                    loc_string = '%s, %s, %s' % (e['city'], e['state'], e['country'])
                    if loc_string in location_cache:
                        location = location_cache[loc_string]
                    else:
                        location = get_coordinates_from_text(loc_string)
                        if location:
                            location_cache[loc_string] = location
                '''
                            
                words = list(e[9:110])
                event = {
                    'id': eid,
                    'words': words
                }
                event_info.insert([event])

        #update ages in event
        user_dict = list(self.user_info.find())
        user_dict = {u['id']:u for u in user_dict}
        count = event_info.count()
        for e in event_info.find():
            attends = self.attendance.find({'eid': e['id']})
            count -= 1
            if count % 10000 == 0:
                print(count)
            ages = []
            sexes = {'male':0, 'female':0}
            for at in attends:
                if 'yes' not in attends:
                    continue
                uid = at['uid']
                if uid in user_dict:
                    a = user_dict[uid]['age']
                    if a:
                        ages.append(a)
            
            if ages:
                age = np.mean(ages)
                print(age)
                break
                event_info.update({'id': e['id']},
                    {'$set': {'age': age}})

        ##update time of events
        chunks = pd.read_csv("events.csv", iterator=True, chunksize=10000)

        count = 0
        for chunk in chunks:
            print(count)
            count += 10000
            for e in chunk.iterrows():
                e = e[1]
                t = e['start_time']
                t = parse(t)
                t = time.mktime(t.timetuple())
                event_info.update({'id': e['event_id']},
                    {'$set': {'start': t}})

    def load_user_info(self):
        print("loading user info to db")
        
        with open('users.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                record = {
                    'uid': int(row['user_id']),
                    'locale': row['locale'],
                    'birthyear': row['birthyear'],
                    'gender': row['gender'],
                    'joinedAt': row['joinedAt'],
                    'location':row['location'],
                    'timezone':row['timezone']

                }
                self.user_info.insert_one(record)
        
        # calculate age of users
        ages={}
        for u in self.user_info.find():
            a = u['birth']
            try:
                a = int(a)
                if a < 1940:
                    a = None
                else:
                    a = 2013 - a
            except:
                a = None
            ages[a] = ages.get(a, 0) + 1
            self.user_info.update({'id': u['id']},
                    {'$set': {'age': a}})

    def load_attendance_info(self):
        print("loading attendance info to db")
        #self.attendance = self.db['attendance_info']
        self.load_attendance_info_2()
        '''
        
        with open('event_attendees.csv',newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            columns  =reader.fieldnames
            print(columns)
            for row in reader:
                record = {}
                for key in columns:
                    record[key]=row[key]
                attendance_info.insert_one(record)
        '''

    
    
    # add attendance

    # add event info
    def load_event_info(self):
        print("loading event info to db")
        event_info = self.db['event_info']
        with open('events.csv',newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            columns = reader.fieldnames
            print(columns)
            for row in reader:
                record = {}
                for key in columns:
                    record[key]=row[key]
                event_info.insert_one(record)

    def update_attendance(self,uid, eid, att_type):
            self.attendance.update(
            {'uid': uid, 'eid': eid},
            {'$set': {'uid': uid, 'eid': eid, att_type: True}}, 
            upsert=True)

    def load_attendance_info_2(self):
        chunks = pd.read_csv("events.csv", iterator=True, chunksize=1000)
        count = 0
        for chunk in chunks:
            print(count)
            count += 1000
            for e in chunk.iterrows():
                e = e[1]
                eid = e['event_id']
                uid = e['user_id']
                # add creator to event attendance collection
                self.update_attendance(uid, eid, 'yes')
                self.update_attendance(uid, eid, 'interested')
        
        train = pd.read_csv( "train.csv")
        for pair in train.iterrows():
            pair = pair[1]
            uid = pair['user']
            eid = pair['event']
            for attr in ['invited', 'interested', 'not_interested']:
                if pair[attr]:
                    self.update_attendance(uid, eid, attr)
        
        event_attendees = pd.read_csv("event_attendees.csv")
        for event in event_attendees.iterrows():
            event = event[1]
            eid = event['event']
            for attr in ['yes', 'maybe', 'invited', 'no']:
                users = event[attr]
                if isinstance(users, float):
                    continue
                users = [int(u) for u in users.split()]
                for uid in users:
                    self.update_attendance(uid, eid, attr)



if __name__ == "__main__":
    loaddata()