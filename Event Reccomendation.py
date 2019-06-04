import random
import time
from math import sqrt
import pandas as pd
import numpy as np
import pymongo as mgo
import json
from pymongo import IndexModel, ASCENDING
from dateutil.parser import parse
from model import Model
from eval import apk

def memoize(function):
  memo = {}
  def wrapper(*args):
    
    if args in memo:
      return memo[args]
    else:
      rv = function(*args)
      memo[args] = rv
      return rv
  return wrapper

mongo = mgo.MongoClient('localhost', 27017)
db = mongo['event-recommendation']
collection = db['event_rec']
event_data = pd.read_csv('event_attendees.csv')
data_json = json.loads(event_data.to_json(orient='records'))

user_info = db.user_info
event_info = db.event_info
attendance = db.attendance_info
friends_db = db.friends_info

user_info.create_index('id')
event_info.create_index('id')
attendance.create_index('uid')
attendance.create_index('eid')
attendance.create_index([('uid', ASCENDING), ('eid', ASCENDING)])
friends_db.create_index('uid')
    
class event_rec():       
    def __init__(self):
        self.X = []
        self.Y1 = []
        self.Y2 = []
        #self.init_mongo()
        #self.X,self.Y1,self.Y2 = self.load_training()
        #self.splits=self.get_crossval_data
        #self.load_test()
        #self.extract_features()
        #self.run_crossval()
        self.run_full()
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

    @memoize
    def get_user_attendance(self,uid):
        return list(attendance.find({'uid': uid}))

    def get_event_attendance(self,eid):
        return list(attendance.find({'eid': int(eid)}))

    # processes the events and return features
    def process_events(self, uid, e_dict):
        #for id, invited in ev.items():
        #    self.user_event_cases(id)
        attend_list_u = self.get_user_attendance(uid)
        attend_list_ids = [f['eid'] for f in attend_list_u]
        #print("len of e_dict is "+str(len(e_dict.keys())))
        #print("events in e_dict is "+str(list(e_dict.keys())))
        keyList = [str(e) for e in list(e_dict.keys())]
        #print(str(keyList))
        e_list = list(event_info.find({'event_id':{'$in': keyList}}))
        user = user_info.find_one({'uid': uid})
        attend_dict = {e['eid']: e for e in attend_list_u}
        if uid==None:
            return
        friend_list=friends_db.find_one({'uid': int(uid)})
        if(friend_list==None):
            friends = None
            friend_ids = set()
        else:
            friends = list(friend_list)
            friend_ids = set(friend_list['friends'])

        features_dict = {}
        #print("len of my elist for that uid is "+str(len(e_list)))
        for e in e_list:
            attend_list_e = self.get_event_attendance(e['event_id'])
            features = [0, 0, 0, 0]
            #print("number of attendees for event "+str(e['event_id'])+" is "+str(len(attend_list_e)))
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
            features.extend([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])
            # add distance to event ...................
            #print(e['event_id'])
            #print('length of features for event '+str(e['event_id'])+" is "+str(len(features)))
            features_dict[e['event_id']] = features
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
        X = []
        Y1 = []
        Y2 = [] 
        count = train_pd['user'].count()
        
        for user, events in train_set.items():
            ev = {ev_id['eid']: ev_id['invited'] for ev_id in events}
            features_dict= self.process_events(user, ev)
            #print(len(X) * 100.0 / count)
            for e in events:
                eid = e['eid']
                if(eid not in features_dict.keys()):
                    continue
                X.append(features_dict[eid])
                Y1.append(e['interested'])
                Y2.append(e['not_interested'])
        print("done loading training data")
        return (X, Y1, Y2)
        
        

    def load_test(self):
        print("Load testing data")
        test_pd = pd.read_csv("test.csv")
        #print(test_pd.shape)
        #print(test_pd.head)
        return

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
        irange = [0, int(len(train_dict) / 320),int(len(train_dict) / 160) ,len(train_dict)]
        print("irange "+str(irange))
        count2=int(len(train_dict) / 160)
        for i in range(2):
            
            X = []
            Y1 = []
            Y2 = []
            results = {}
            keys = []
            count = train['user'].count()
            uids = list(train_dict.keys())
            uids = uids[irange[i] : irange[i + 1]]
            events = list(train_dict.values())
            events = events[irange[i] : irange[i + 1]]
            mytup = zip(uids,events)
            for uid, events in mytup:
                print(count2)
                count2-=1
                e_dict = {e['eid']: (e['invited'], e['timestamp']) for e in events}
                features_dict = self.process_events(uid, e_dict)
                if random.random() < 0.1:
                    print(len(X) * 100.0 / count)
                    
                results[uid] = []
                for e in events:
                    eid = e['eid']
                    #print("my key is "+str(eid))
                    #print("my keys in features dict is "+str(features_dict.keys()))
                    if str(eid) not in features_dict.keys():
                        print(eid)
                        #print("key not found")
                        continue
                    #print("key found")
                    value = features_dict[str(eid)]
                    X.append(value)
                    
                    Y1.append(e['interested'])
                    Y2.append(e['not_interested'])
                    keys.append((uid, e['eid']))
                    if e['interested']:
                        results[uid].append(e['eid'])
                
            #print(len(X))
            splits.append((X, Y1, Y2, results, keys))
            #print(features_dict[0])
        return splits
    
    def run_crossval(self):
        splits = self.get_crossval_data()
        print("length of splits "+str(len(splits)))
        print("length of splits of 0 "+str(len(splits[0])))
        print("length of splits of 0 of 0 : "+str(len(splits[0][0])))
        results = []
        for i in range(2):
            s = splits[i]
            other_s = splits[1 - i]
            
            z = [True] * len(s[0])
            w = [True] * len(s[0])
            print(len(z))
            remove_features_rfc = [19,20]
            remove_features_lr = [19,20,21,22,23,24,25,26,29,30,31,32]
            
            for i in remove_features_rfc:
                z[i] = False
            for i in remove_features_lr:
                w[i] = False
            
            m1 = Model(compress=z, has_none=w)
            m1.fit(s[0], s[1])
            
            X = other_s[0]
            predictions = m1.test(X)
            keys = other_s[4]
            pred_dict = {}
            for j in xrange(len(keys)):
                uid, eid = keys[j]
                if uid not in pred_dict:
                    pred_dict[uid] = []
                pred_dict[uid].append((eid, predictions[j]))
                
            for uid, l in pred_dict.items():
                l.sort(key=lambda x: -x[1])
                l = [e[0] for e in l]
                results.append(apk(other_s[3][uid], l))
        
        print("done running cross val")
        #print(sum(results) / len(results))

    def run_full(self):
        splits = self.get_crossval_data()
        print("length of x "+str(len(splits[0][0])))
        print("length of y1 "+str(len(splits[0][1])))
        print("length of y2 "+str(len(splits[0][2])))
        X = splits[0][0] + splits[1][0]
        Y1 = splits[0][1] + splits[1][1]
        Y2 = splits[0][2] + splits[1][2]
        test_data = self.get_test_data()
        
        remove_features_rfc = [19,20,34]
        remove_features_lr = [19,20,21,22,23,24,25,26,29,30,31,32,34]
        
        not_useful_rfc = [8,11,22,24,28,33,30,31,32]#9,21#30,31,32
        remove_features_rfc.extend(not_useful_rfc)
        not_useful_lr = [3,4,9,11,14,15,16,17,27,28,30,31,32]
        remove_features_lr.extend(not_useful_lr)
            
        z = [True] * len(X[0])
        w = [True] * len(X[0])
        
        for i in remove_features_rfc:
            z[i] = False
        for i in remove_features_lr:
            w[i] = False
        
        C = 0.03
        #C = 0.3
        print("initializing model")
        m1 = Model(compress=z, has_none=w, C=C)
        print("fitting model")
        m1.fit(X, Y1)
        #TODO save model to db
        final = False
        results = self.run_model(m1, None, test_data, is_final=final)
        print("done till here")
        print(results)
        
        if not final:
            print(self.evaluate_test_results(results))
        self.write_submission('output.csv', results)

    def get_test_data(self):
        solutions_dict = self.get_test_solutions()
        test = pd.read_csv("test.csv")
        test_dict = {}
        
        for record in test.iterrows():
            record = record[1]
            uid = record['user']
            if uid not in solutions_dict:
                continue
            if uid not in test_dict:
                test_dict[uid] = []
            test_dict[uid].append({
                'eid': record['event'],
                'invited': record['invited'],
                'timestamp': time.mktime(parse(record['timestamp']).timetuple()),
            })
        print("batches of test")
        irange = [0, int(len(test_dict) / 40),int(len(test_dict) / 20) ,len(test_dict)]  
        print("irange "+str(irange)) 
        count = int(len(test_dict) / 20)
        #print("the length of my test dict is "+str(count))
        test_data = {}
        for i in range(2):
            uids = list(test_dict.keys())
            uids = uids[irange[i] : irange[i + 1]]
            events = list(test_dict.values())
            events = events[irange[i] : irange[i + 1]]
            mytup = zip(uids,events)
            
            for uid, events in mytup:
                print(count)
                count-=1
                e_dict = {e['eid']: (e['invited'], e['timestamp']) for e in events}
                features_dict = self.process_events(uid, e_dict)
                X = []
                for e in events:
                    eid = e['eid']
                    if str(eid) not in features_dict.keys():
                        continue
                    X.append(features_dict[str(eid)])
                test_data[uid] = { 
                    'X': X,
                    'events': events
                }
           
        return test_data
    
    def evaluate_test_results(self,my_results):
        solutions_dict = self.get_test_solutions()
        scores = []
        for uid, l in my_results.items():
            score = apk(solutions_dict[uid], l)
            scores.append(score)
        return sum(scores) / len(scores)
    
    def get_test_solutions(self):
        # read solutions
        solutions_file = pd.read_csv("public_leaderboard_solution.csv")
        solutions_dict = {}
        for row in solutions_file.iterrows():
            row = row[1]
            uid = int(row['User'])
            eid = int(row['Events'])
            solutions_dict[uid] = [eid]
        return solutions_dict

    def run_model(self,m1, m2, test_data, is_final=True):
        final_dict = self.get_final_data()
        results = {}
        for uid, record in test_data.items():
            if is_final and uid not in final_dict:
                continue
            X = record['X']
            events = record['events']
            Y1 = m1.test(X)
            final = Y1
            sorted_events = []
            for i, e in enumerate(events):
                score = final[i]
                sorted_events.append((e['eid'], score))
            sorted_events.sort(key=lambda x: -x[1])
            sorted_events = [e[0] for e in sorted_events]
            results[uid] = sorted_events
    
        return results
    
    def get_final_data(self):
        final_file = pd.read_csv("event_popularity_benchmark.csv")
        final_dict = {}
        for row in final_file.iterrows():
            row = row[1]
            uid = int(row['User'])
            eid = eval(row['Events'])
            if uid in final_dict:
                raise Exception
            final_dict[uid] = eid
        return final_dict
    
    def write_submission(self,submission_name, user_events_dict):
        users = sorted(user_events_dict)
        events = [' '.join([str(s) for s in user_events_dict[u]]) for u in users]

        submission = pd.DataFrame({"User": users, "Events": events})
        submission[["User", "Events"]].to_csv(submission_name, index=False)
    
if __name__ == "__main__":
    #create_db()
    event_rec()

    
