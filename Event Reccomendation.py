import random
import time
from math import sqrt
import pandas as pd
import numpy as np
import pymongo as mgo
import json


def create_db():
    print("connecting to mongo db..")
    mongo = mgo.MongoClient('localhost', 27017)
    db = mongo['event_rec']
    collection = db['event_rec']
    event_data = pd.read_csv('event_attendees.csv')
    data_json = json.loads(event_data.to_json(orient='records'))
    
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

    def init_mongo(self):
        print("init mongo")
        socket = mgo.Connection()
        db = socket.recomend

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


    # processes the events and return features
    def process_events(self, user, ev):
        for id, invited in ev.items():
            self.user_event_cases(id)



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

    

    
