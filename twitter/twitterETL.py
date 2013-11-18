import boto
import urllib2
import json
from boto.s3.connection import S3Connection

def main():
        conn = S3Connection('AKIAIUUSLBP7GFHBNSDA','PDCZYJMlo6C4JnyWTMybv2XhDLbZhdtHsIjoTU20')
#conn = boto.connect_s3()
        mybucket = conn.get_bucket('15619twitter')
        set = mybucket.list()

        for i in set:
                s=i.name
                break

        s = "http://s3.amazonaws.com/15619twitter/sample-tweets.20131001-162934.json"
        response = urllib2.urlopen(s)
        tweets = response.read().split('\n')
 #user.id  id text time retweet_status(false) id       
        num = 0
        f = open('tweets','w')
        for tweetJason in tweets:
                if tweetJason == "":
                   continue
                tweet = json.loads(tweetJason)
                userid = tweet['user']['id']
                tid = tweet['id']
                text = tweet['text']
                timestamp = tweet['created_at']
                if(tweet.has_key('retweeted_status')):
                        o_userid = tweet['retweeted_status']['user']['id']
                else:
                        o_userid = "false"
                f.write(json.dumps({'userid':userid, 'tid':tid, 'text':text, 'timestamp':timestamp, 'o_userid':o_userid})+'\n')

if __name__=='__main__':
    main()
