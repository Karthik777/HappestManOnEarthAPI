'''
Created on 14 May 2015

@author: jinyoung

nohup mpirun -n 3 python ./Python/getProlificUsers.py -a "http://144.6.227.96:5984" -n perth -v "_design/groupview/_view/groupview" -f token_key/ -r ./Result > getProlificUsers.out&
'''
import couchdb
from optparse import OptionParser
import json
import os
from mpi4py import MPI
import fnmatch
import tweepy
import time

def getMost(view, sentiment):
    maxCnt = 0
    found = None
    for row in view:
        if sentiment in row.key:
            if row.value > maxCnt:
                maxCnt = row.value
                found = row
    return found


def getMostbyYear(view, sentiment, t_year):
    maxCnt = 0
    found = None
    for row in view:
        if sentiment in row.key:
            if t_year in row.key:
                if row.value > maxCnt:
                    maxCnt = row.value
                    found = row
    return found

def getMostbyYearTime(view, sentiment, t_year, t_clock):
    maxCnt = 0
    found = None
    for row in view:
        if sentiment in row.key:
            if t_year in row.key:
                if t_clock in row.key:
                    if row.value > maxCnt:
                        maxCnt = row.value
                        found = row
    return found

def getEmptyJSON():
    result = {
            'user': None,
            'sentiment': None,
            'year': None,
            'time': None,
            'counts': 0
    }
    json_data = json.loads(json.dumps(result, sort_keys=True))
    return json_data

def getUserDetails(auth, uid):
    api = tweepy.API(auth)
    try:
        user_info = api.get_user(user_id=uid)
        time.sleep(5)
        return user_info
    except:
        time.sleep(5)
        return None

def getLatestTweet(auth, uid):
    api = tweepy.API(auth)
    try:
        timeline = api.user_timeline(user_id=uid, count=1, include_rts=False)
        time.sleep(5)
        return timeline[0]
    except:
        time.sleep(5)
        return None

def main():
    db_addr = None
    db_name = None
    view_name = None
    result_dir_path = os.getcwd() + "/results"
    
    usage = "usage: SearchUser -a db_address -n db_name -v view_name -f key_token_folder"
    parser = OptionParser(usage)
    parser.add_option("-a", "--address", type="string", dest="addr", help="DB address")
    parser.add_option("-n", "--db_name", type="string", dest="dname", help="DB name")
    parser.add_option("-v", "--view_name", type="string", dest="vname", help="View name")
    parser.add_option("-f", "--token_folder", dest="token_folder", help="Token key folder")
    parser.add_option("-r", "--result_folder", dest="result_folder", help="Result folder")
    
    opts, args = parser.parse_args()
    if opts.addr:
        db_addr = opts.addr
    if opts.dname:
        db_name = opts.dname
    if opts.vname:
        view_name = opts.vname
    
    auths = []
    if opts.token_folder:
        for root, dirs, files in os.walk(opts.token_folder):
            for token_file in fnmatch.filter(files, "*.t"):
                lines = [line.strip() for line in open(os.path.join(root, token_file))]
                auth = tweepy.OAuthHandler(lines[0], lines[1])
                auth.set_access_token(lines[2], lines[3])
                auths.append(auth)
    
    if opts.result_folder:
        result_dir_path = opts.result_folder
    
    
    couch = couchdb.Server(db_addr)
    db=couch[db_name]
    
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    hostname = MPI.Get_processor_name()
    
    sentiments = ["Negative", "Neutral", "Positive"]
    years = ["2010", "2011", "2012", "2013", "2014", "2015"]
    
    output_file_name = None
    
    if rank == 0:
        output_file_name = "prolific_user_thoughout.txt"
        view = db.view(view_name, group_level=rank+2)
        json_list = []
        for sentiment in sentiments:
            found = getMost(view, sentiment)
            if found != None:
                json_data = getEmptyJSON()
                '''
                user = getUserDetails(auths[rank], found.key[0])
                if user == None:
                    json_data['user'] = None
                else:
                    #json_data['user'] = found.key[0]
                    json_data['user'] = user._json
                '''
                tweet = getLatestTweet(auths[rank], found.key[0])
                if tweet == None:
                    json_data['user'] = None
                else:
                    tweet_json = tweet._json
                    json_data['user'] = tweet_json['user']
                    
                json_data['sentiment'] = found.key[1]
                json_data['counts'] = found.value
                json_list.append(json_data)
            
            
    if rank == 1:
        output_file_name = "prolific_user_of_year.txt"
        view = db.view(view_name, group_level=rank+2)
        json_list = []
        for sentiment in sentiments:
            for year in years:
                found = getMostbyYear(view, sentiment, year)
                if found != None:
                    json_data = getEmptyJSON()
                    '''
                    user = getUserDetails(auths[rank], found.key[0])
                    if user == None:
                        json_data['user'] = None
                    else:
                        #json_data['user'] = found.key[0]
                        json_data['user'] = user._json
                    '''
                    tweet = getLatestTweet(auths[rank], found.key[0])
                    if tweet == None:
                        json_data['user'] = None
                    else:
                        tweet_json = tweet._json
                        json_data['user'] = tweet_json['user']
                        
                    json_data['sentiment'] = found.key[1]
                    json_data['year'] = found.key[2]
                    json_data['counts'] = found.value
                    json_list.append(json_data)
                
                
    if rank == 2:
        output_file_name = "prolific_user_of_time.txt"
        view = db.view(view_name, group_level=rank+2)
        json_list = []
        for sentiment in sentiments:
            for year in years:
                for clock in range(0, 24):
                    found = getMostbyYearTime(view, sentiment, year, clock)
                    if found != None:
                        json_data = getEmptyJSON()
                        '''
                        user = getUserDetails(auths[rank], found.key[0])
                        if user == None:
                            json_data['user'] = None
                        else:
                            #json_data['user'] = found.key[0]
                            json_data['user'] = user._json
                        '''
                        tweet = getLatestTweet(auths[rank], found.key[0])
                        if tweet == None:
                            json_data['user'] = None
                        else:
                            tweet_json = tweet._json
                            json_data['user'] = tweet_json['user']
                            
                        json_data['sentiment'] = found.key[1]
                        json_data['year'] = found.key[2]
                        json_data['time'] = found.key[3]
                        json_data['counts'] = found.value
                        json_list.append(json_data)
    
    if rank == 0 or rank == 1 or rank == 2:
        #result_dir_name = "results"
        #result_dir_path = os.getcwd() + "/" + result_dir_name
        
        if not os.path.exists(result_dir_path):
            os.mkdir(result_dir_path)
    
        os.chdir(result_dir_path)
    
        if os.path.exists(output_file_name):
            os.remove(output_file_name)
    
        outputfile = open(output_file_name, "a")
        for json_data in json_list:
            json.dump(json_data, outputfile)
            outputfile.write("\n")
        outputfile.close()
    
    print "rank: %d (hostname: %s) completed." % (rank, hostname)
        
if __name__ == '__main__':
    main()