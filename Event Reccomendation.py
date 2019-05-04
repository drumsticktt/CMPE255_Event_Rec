import random
import time
from math import sqrt
import pandas as pd
import numpy as np
import pymongo as mgo
import json
from pymongo import MongoClient


def create_db():
    print("connecting to mongo db..")
    mongo = MongoClient('localhost', 27017)
    db = mongo['event-recommendation']
    user_info = db.user_info
    event_info = db.event_info
    attendance = db.attendance_info
    friends_db = db.friends

    user_info.ensure_index('id', {'unique': True})
    event_info.ensure_index('id', {'unique': True})
    attendance.ensure_index('uid')
    attendance.ensure_index('eid')
    attendance.ensure_index([('uid', ASCENDING), ('eid', ASCENDING)])
    friends_db.ensure_index('uid')

    #db = mongo['event_rec']
    #collection = db['event_rec']
    #event_data = pd.read_csv('event_attendees.csv')
    #data_json = json.loads(event_data.to_json(orient='records'))
    
class event_rec():       
    def __init__(self):
        #this.x = []
        #this.Y1 = []
        #this.Y2 = []
        #self.init_mongo()
        #self.load_training()
        #self.load_test()
        #self.extract_features()
        return

    # def init_mongo(self):
    #     print("init mongo")
    #     socket = mgo.Connection()
    #     db = socket.recomend

    def preprocess(self):
        return

    def train_model(self):
        return
    def load_model(self):
        return False 

    def extract_features(self):
        '''
        Feature Extraction Order:
        User Attendence
        
        '''
        events_pd = pd.read_csv("events.csv")
        print(events_pd.shape)
        print(events_pd.head)
        for e in events_pd:
            for att in e['id']:
                # do some work
                pass

    #number of users attending, not attending, maybe attending and invited to the event
    # with given event id
    def user_event_cases(self, eid):
       # TODO
       pass
    
    @memoize
    def get_user_attendance(self, uid):
        return list(attendance.find({'uid': uid}))

    
    def get_event_attendance(self, eid):
        return list(attendance.find({'eid': int(eid)}))
    
    # processes the events and return features
    def process_events(self, uid, e_dict):

        attend_list_u = get_user_attendance(uid)
        attend_list_ids = [f['eid'] for f in attend_list_u]
        e_list = list(event_info.find({'id':{'$in': e_dict.keys()}}))
        user = user_info.find_one({'id': uid})
        attend_dict = {e['eid']: e for e in attend_list_u}
        
        friends = list(friends_db.find_one({'uid': int(uid)}))
        friend_ids = set(friends['friends'])

        features_dict = {}
        for e in e_list:
            attend_list_e = get_event_attendance(e['id'])
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

            # age profile 

            # gender profile












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
        

    def load_test(self):
        print("Load testing data")
        test_pd = pd.read_csv("test.csv")
        #print(test_pd.shape)
        #print(test_pd.head)
        return
    
if __name__ == "__main__":
    create_db()
    event_rec()

    

    
