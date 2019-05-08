import random
import time
from math import sqrt
import pandas as pd
import numpy as np
import pymongo as mgo
from dateutil.parser import parse
import csv
import json



mongo = mgo.MongoClient('localhost', 27017)
db = mongo['event-recommendation']
collection = db['event_rec']
event_data = pd.read_csv('event_attendees.csv')
data_json = json.loads(event_data.to_json(orient='records'))




class event_rec():

    
    
    def __init__(self):
        self.user_info = []
        self.event_info = []
        self.attendance = []
        self.friends_db = []
        self.load_user_info()
        self.load_event_info_2()
        self.load_attendance_info_2()
        self.load_friends()
        self.get_crossval_data()
        self.load_test()
        return




    def load_friends(self):
        print("loading user friends info to db")
        with open('user_friends.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                record = {
                    'uid': row['user'],
                    'friends': row['friends']
                }
                self.friends_db.append(record)

    def load_user_info(self):
        print("loading user info into ram")
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
                self.user_info.append(record)


    def load_event_info_2(self):
        print("loading event info to ram")
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
                            
                words = list(e[9:110])
                event = {
                    'id': eid,
                    'words': words
                }
                self.event_info.append(event)
                
    def update_attendance(self,uid, eid, att_type):
        ex = [att for att in self.attendance if att['uid'] == uid and att['eid'] == eid]
        for att in ex:
            att[att_type] = True
            return
        self.attendance.append({'uid': uid, 'eid': eid, att_type: True})

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

    def init_mongo(self):
        print("init mongo")
        socket = mgo.Connection()
        db = socket.recomend

    def get_event_sim_by_users(id1, id2, exclude):
        attend1 = attendance.find({'eid': id1})
        attend2 = attendance.find({'eid': id2})
        set1 = set([])
        set2 = set([])
        for a in attend1:
            if 'interested' in a or 'yes' in a:
                set1.add(a['uid'])
        for a in attend2:
            if 'interested' in a or 'yes' in a:
                set2.add(a['uid'])
        
        intersection = set1.intersection(set2)
        s = float(len(intersection))
        if exclude in intersection:
            s -= 1
        
        s /= min(len(set1), len(set2))
        
        return s
        
    def get_event_sim_by_cluster(user, event):
        f = []
        for key in ['user_taste', 'friends_taste', 'user_hates', \
                'friends_hate', 'user_invited']:
            if key in user:
                s = 0
                taste = user[key]
                s += taste['cl0'][event['cl0']] * 8
                s += taste['cl1'][event['cl1']] * 20
                s += taste['cl2'][event['cl2']] * 40
                f.append(s)
            else:
                f.append(None)
                #f.append(0 if key in ['user_hates', 'friends_hate'] else None)
        return f

    def get_user_attendance(self,uid):
        return [k for k in self.attendance if k['uid'] == uid]

    def get_event_attendance(self,eid):
        return [k for k in self.attendance if k['eid'] == eid]

    # processes the events and return features
    def process_events(self, uid, e_dict):
        #for id, invited in ev.items():
        #    self.user_event_cases(id)
        attend_list_u = self.get_user_attendance(uid)
        attend_list_ids = [f['eid'] for f in attend_list_u]
        e_list = [ev for ev in self.event_info if ev['id'] in e_dict.keys()]
        user = [ u for u in self.user_info if u['id'] == uid]
        attend_dict = {e['eid']: e for e in attend_list_u}

        friends = [f for f in self.friends_db if f['uid'] == int(uid)]
        friend_ids = [f['friends'] for f in friends]

        features_dict = {}
        for e in e_list:
            attend_list_e = self.get_event_attendance(e['id'])
            features = [0, 0, 0, 0]
            for att in attend_list_e:
                if 'yes' in att:
                    features[0] += 1
                if 'no' in att:
                    features[1] += 1
                if 'maybe' in att:
                    features[2] += 1
                if 'invited' in att:
                    features[3] += 1
            features.extend([
                features[1] * 1.0 / (features[0] + 1),
                features[2] * 1.0 / (features[0] + 1),
                features[3] * 1.0 / (features[0] + 1),
            ])
            
            features2 = [0, 0, 0, 0]
            for att in attend_list_e:
                if att['uid'] not in friend_ids:
                    continue
                if 'yes' in att:
                    features2[0] += 1
                if 'no' in att:
                    features2[1] += 1
                if 'maybe' in att:
                    features2[2] += 1
                if 'invited' in att:
                    features2[3] += 1
            features2.extend([
                features2[1] * 1.0 / (features2[0] + 1),
                features2[2] * 1.0 / (features2[0] + 1),
                features2[3] * 1.0 / (features2[0] + 1),
            ])
            features2.extend([
                features2[0] / (len(friend_ids) + 1.0),
                features2[1] / (len(friend_ids) + 1.0),
                features2[2] / (len(friend_ids) + 1.0),
                features2[3] / (len(friend_ids) + 1.0),
            ])
            features.extend(features2)

            # add distance to event
        
            features.extend(
                process_locations(
                    e.get('newloc2', []), 
                    user.get('newloc2', [])
                )
            )
        
            # add event similarity by user
            features.append(get_event_similarity_by_user_big(uid, e['id']))
        
            # add user to event cluster sim
            features.extend(get_event_sim_by_cluster(user, e))
        
            # see if event creator is a friend - ar putea cauza scadere de 0.2%
            features.append(e['creator'] in friend_ids)
        
            # compare to what the user's friends like
            if 'prototype' in user:
                features.append(get_event_distance(user['prototype'], e['words']))
            else:
                features.append(None)
            
            if 'prototype' in user and 'prototype_invite' in user:
                v1 = get_event_distance(user['prototype'], e['words'])
                v2 = get_event_distance(user['prototype_invite'], e['words'])
                if v1 and v2:
                    features.append(v1 - v2)
                else:
                    features.append(None)
            else:
                features.append(None)
            if 'prototype_hate' in user and 'prototype_invite' in user:
                v1 = get_event_distance(user['prototype_hate'], e['words'])
                v2 = get_event_distance(user['prototype_invite'], e['words'])
                if v1 and v2:
                    features.append(v1 - v2)
                else:
                    features.append(None)
            else:
                features.append(None)
            if 'prototype_hate' in user and 'prototype' in user:
                v1 = get_event_distance(user['prototype_hate'], e['words'])
                v2 = get_event_distance(user['prototype'], e['words'])
                if v1 and v2:
                    features.append(v1 - v2)
                else:
                    features.append(None)
            else:
                features.append(None)
        
            # add invited flag
            features.append(e_dict[e['id']][0])
        
            # old locations
            features.append(get_location_distance(e['location'], user['location']))
            
            features_dict[e['id']] = features
        
        # normalize location score
        '''
        lind = 14 # the index of the position score in the feature vector
        scores = [f[lind] for f in features_dict.itervalues()]
        m = max(scores)
        if m:
            for k in features_dict:
                f = features_dict[k][lind] / m
                #f = f**3
                features_dict[k][lind] = f
        '''
        return features_dict

    def load_training(self):
        print("Load training data")
        train_pd = pd.read_csv("train.csv")
        #print(train_pd.shape)
        #print(train_pd.head)
        train_set = {}
        for event in train_pd.iterrows():
            event = event[1]
            user = event['user']
            if user not in train_set:
                train_set[user] = []
            train_set[user].append({
                'eid': event['event'],
                'invited':event['invited'],
                'interested': event['interested'],
                'not interested': event['not_interested']})

            user_num = train_pd['user'].count()
            for user, events in train_set.items():
                ev = {ev_id['eid']: ev_id['invited'] for ev_id in events}
                self.process_events(user, ev)



    def get_crossval_data(self):
        train = pd.read_csv( "train.csv")
        train_dict = {}
        duplicates = set([])
        for record in train.iterrows():
            record = record[1]
            uid = record['user']
            
            # check for duplicates
            key = (uid, record['event'])
            if key in duplicates:
                continue
            duplicates.add(key)
            
            if uid not in train_dict:
                train_dict[uid] = []
            train_dict[uid].append({
                'eid': record['event'],
                'invited': record['invited'],
                'interested': record['interested'],
                'not_interested': record['not_interested'],
                'timestamp': time.mktime(parse(record['timestamp']).timetuple()),
            })
          
        splits = []
        irange = [0, len(train_dict) / 2, len(train_dict)]
        for i in range(2):
            X = []
            Y1 = []
            Y2 = []
            results = {}
            keys = []
            count = train['user'].count()
            print(len(train_dict))
            for uid, events in train_dict.items():
                e_dict = {e['eid']: (e['invited'], e['timestamp']) for e in events}
                features_dict = self.process_events(uid, e_dict)
                if random.random() < 0.1:
                    print (len(X) * 100.0 / count)
                results[uid] = []
                for e in events:
                    eid = e['eid']
                    X.append(features_dict[eid])
                    Y1.append(e['interested'])
                    Y2.append(e['not_interested'])
                    keys.append((uid, e['eid']))
                    if e['interested']:
                        results[uid].append(e['eid'])
            
            splits.append((X, Y1, Y2, results, keys))
            
        return splits

        

    def load_test(self):
        print("Load testing data")
        test_pd = pd.read_csv("test.csv")
        #print(test_pd.shape)
        #print(test_pd.head)
        return
    
if __name__ == "__main__":
    #create_db()
    event_rec()

    
