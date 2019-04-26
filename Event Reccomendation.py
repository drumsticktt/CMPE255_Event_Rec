import random
import time
from math import sqrt
import pandas as pd
import numpy as np
import pymongo as mgo


def create_db():
    print("connecting to mongo db..")
    mongo = mgo.MongoClient('localhost', 27017)
    db = mongo['event_rec']
    collection = db['event_rec']
    
class event_rec():       
    def __init__(self):
        self.path = "event-recommendation-engine-challenge"
        #this.x = []
        #this.Y1 = []
        #this.Y2 = []
        #self.init_mongo()
        self.load_training()
        self.load_test()
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
    
    def load_training(self):
        print("load training data")
        train_pd = pd.read_csv(self.path+"/train.csv")
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
                #features = 
        

    def load_test(self):
        print("load testing data")
        test_pd = pd.read_csv(self.path+"/test.csv")
        return
    
if __name__ == "__main__":
    create_db()
    event_rec()

    

    
