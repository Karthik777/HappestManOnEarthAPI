'''
Created on 13 May 2015

@author: jinyoung

nohup mpirun -n 4 --hostfile ./hostfile python mpi_crawler.py -a "http://144.6.227.96:5984" -n "test" -f ./token_key > mpi_crawler.out & 
'''
import couchdb
import tweepy
from tweepy.streaming import json
import time
from optparse import OptionParser
#from multiprocessing import Process
from mpi4py import MPI
import os
import fnmatch
import unirest

city_name = "Perth"
language = "en"
city_geocode = "-31.951667,115.860001,70km"
location_perth = [115.402624, -32.521749, 116.388647, -31.396259]
dayinSeconds = 86400

#sentiment_url = "http://sentiment.vivekn.com/api/text/"
#sentiment_headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

def getSentiment(text):
    response = unirest.post("http://sentiment.vivekn.com/api/text/",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        },
        params={
            "txt": text
        }
    )
    return response.body["result"]["sentiment"]

def storeJSON(json, db):
    try:
        if json.has_key("lang") and json["lang"] == language:
            json["_id"] = "%d" % json["id"]
            # sentiment analysis
            sentiment_val = getSentiment(json["text"])
            json["sentiment"] = "%s" % sentiment_val 
            
            db.save(json)
            return True
        else:
            return False
    except:
        return False

def get_history_pages(auth):
    api = tweepy.API(auth)
    try:
        pages = []
        for page in tweepy.Cursor(api.search, geocode=city_geocode, lang=language, count=100).pages():
            pages.append(page)
            time.sleep(2)
        return pages
    except:
        time.sleep(2)
        return None

def get_timeline_pages(auth, uid):
    api = tweepy.API(auth)
    try:
        pages = []
        for page in tweepy.Cursor(api.user_timeline, user_id=uid, count=200, include_rts=True).pages():
            pages.append(page)
            time.sleep(3)   
        return pages
    except:
        #print "Not Authorized at %d" % uid
        time.sleep(3)
        return None

def get_friend_ids(auth, uid):
    api = tweepy.API(auth)
    try:
        friend_ids = []
        for page in tweepy.Cursor(api.friends_ids, user_id=uid, count=5000).pages():
            friend_ids.extend(page)
            time.sleep(60)
        return friend_ids
    except:
        time.sleep(60)
        return None
    
def storeUserTimeline(pages, db):
    for page in pages:
        for item in page:
            item_json = item._json
            if item_json["place"] != None:
                if item_json["place"]["name"] == city_name:
                    storeJSON(item_json, db)
            else:   # tweet["place"] == None
                if item_json["user"]["time_zone"] == None or item_json["user"]["time_zone"] == city_name:
                    storeJSON(item_json, db)

def storeFriendsTimeline(pages, db):
    for page in pages:
        for item in page:
            item_json = item._json
            if item_json["place"] != None:
                if item_json["place"]["name"] == city_name:
                    storeJSON(item_json, db)
            else:   # item_json["place"] == None
                if item_json["user"]["time_zone"] == city_name:
                    storeJSON(item_json, db)

def startHistoricalSearch(db_addr, db_name, auth):
    couch_server = couchdb.Server(db_addr)
    db = None
    try:
        db = couch_server[db_name]
    except:
        db = couch_server.create(db_name)

    while True:
        # time spent on retrieval
        tStart = time.time()

        history_pages = get_history_pages(auth)
        seed_user_ids = []
        if history_pages != None:
            for history_page in history_pages:
                for item in history_page:
                    item_json = item._json
                    seed_user_ids.append(item_json["user"]["id"])
                    storeJSON(item_json, db)
                    timeline_pages = get_timeline_pages(auth, item_json["user"]["id"])
                    if timeline_pages != None:
                        storeUserTimeline(timeline_pages, db)
    
        ## frined list and their timeline
        if len(seed_user_ids) != 0:
            for user_id in seed_user_ids:
                friend_ids = get_friend_ids(auth, user_id)
                if friend_ids != None:
                    for friend_id in friend_ids:
                        friends_timeline_pages = get_timeline_pages(auth, friend_id)
                        if friends_timeline_pages != None:
                            storeFriendsTimeline(friends_timeline_pages, db)
        
        tEnd = time.time() - tStart
        
        if tEnd < dayinSeconds:
            time.sleep(dayinSeconds-tEnd)

class StreamCrawler(tweepy.StreamListener):
    def __init__(self, auth, db):
        '''
        Constructor
        '''
        self.db = db
        self.auth = auth
        
    def on_data(self, raw_data):
        json_data = json.loads(raw_data)
        if json_data["place"]["name"] == city_name:
            storeJSON(json_data, self.db)
            timeline_pages = get_timeline_pages(self.auth, json_data["user"]["id"])
            if timeline_pages != None:
                storeUserTimeline(timeline_pages, self.db)
            
        ## friend list and their timeline
        friend_ids = get_friend_ids(self.auth, json_data["user"]["id"])
        if friend_ids != None:
            for friend_id in friend_ids:
                friends_timeline_pages = get_timeline_pages(self.auth, friend_id)
                if friends_timeline_pages != None:
                    storeFriendsTimeline(friends_timeline_pages, self.db)
                    
        return True
    
    def on_status(self, status):
        print status.text
    
    def on_error(self, status_code):
        print "Error with status_code: %s" % status_code

def startStreamingSearch(db_addr, db_name, location_pos, auth):
    couch_server = couchdb.Server(db_addr)
    #db = couch_server[db_name]
    db = None
    try:
        db = couch_server[db_name]
    except:
        db = couch_server.create(db_name)
    
    stream_listener = StreamCrawler(auth, db)
    stream = tweepy.Stream(auth, stream_listener)
    stream.filter(locations=location_pos, languages=[language])

def main():
    db_addr = None
    db_name = None
    auths = []
    
    usage = "usage: mpirun -n [# of tokens] --hostfile ./hostfile python mpi_crawler.py -a db_address -n db_name -f key_token_folder"
    parser = OptionParser(usage)
    parser.add_option("-a", "--address", type="string", dest="addr", help="DB address")
    parser.add_option("-n", "--name", type="string", dest="name", help="DB name")
    parser.add_option("-f", "--token_folder", dest="token_folder", help="Token key folder")
    
    opts, args = parser.parse_args()
    if opts.addr:
        db_addr = opts.addr
    if opts.name:
        db_name = opts.name
    if opts.token_folder:
        for root, dirs, files in os.walk(opts.token_folder):
            for file in fnmatch.filter(files, "*.t"):
                lines = [line.strip() for line in open(os.path.join(root, file))]
                auth = tweepy.OAuthHandler(lines[0], lines[1])
                auth.set_access_token(lines[2], lines[3])
                auths.append(auth)
    
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    hostname = MPI.Get_processor_name()
    
    print "Number of tokens: %d" % len(auths)
    if len(auths) == 1:
        if rank == 0:
            print "rank: %d  hostname: %s starts Historical Search" % (rank, hostname)
            startHistoricalSearch(db_addr, db_name, auths[rank])
    elif len(auths) > 1:
        if rank == 0:
            print "rank: %d  hostname: %s starts Historical Search" % (rank, hostname)
            startHistoricalSearch(db_addr, db_name, auths[rank])
        elif rank < len(auths):
            print "rank: %d  hostname: %s starts Streaming Search" % (rank, hostname)
            startStreamingSearch(db_addr, db_name, location_perth, auths[rank])
        else:
            print "rank: %d  hostname: %s (No tokens available)" % (rank, hostname)
    else:
        print "No tokens available"
    
if __name__ == '__main__':
    main()