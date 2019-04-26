import random
import time
from math import sqrt
import pandas as pd
import numpy as np
import pymongo as mgo


def create_db():
    mongo = mgo.MongoClient('localhost', 27017)
    db = mongo['event_rec']
    collection = db['event_rec']
    

class event_rec():
    def init_mongo(self):
        socket = mgo.Connection()
        db = socket.recomend
        
    
    def __init__(self):
        this.x = []
        this.Y1 = []
        this.Y2 = []
        return

    def preprocess(self):
        return

    def train_model(self):
        return
    def load_model(self):
        return False
    
    def load_training(self, path):
        train_pd = pd.read_csv(path)
        train_set = {}
        for event in train_pd.iterrows():
            event = event[1]
            user = record['user']
            if user not in train_set:
                train_set[user] = []
            train_set[user].append({
                'eid': event['event'],
                'invited':event['invited'],
                'interested': event['interested'],
                'not interested': event['not_interested']})

            user_num = train_pd['user'].count()
            for user, events in train_set.iteritems():
                ev = {ev_id['eid']: ev_id['invited'] for ev_id}
                
                features = 
        return

    def load_test(self):
        return

    
