import configparser
import json
import os
import subprocess
import time
import urllib.parse
from collections import deque
from datetime import datetime

import cherrypy
from pymongo import MongoClient

# region API
class App(object):
    @cherrypy.expose
    def index(self):
        return "nothing to see here"


class Tweet(object):
    exposed = True

    def __init__(self, handler):
        self.handler = handler

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        return "stuff"

    @cherrypy.tools.accept(media='text/plain')
    def POST(self, tweet):
        print(tweet)
        tweet = urllib.parse.unquote(tweet)
        # print(tweet)
        self.handler.add_tweet(json.loads(tweet))

#endregion

#region businesslayer
class Storage(object):
    def __init__(self, maximum):
        self.storage = deque([], maximum)

    def add_tweet(self, tweet):
        # first in first out max 5k tweets (maximum)
        # https://docs.python.org/3/library/collections.html#collections.deque
        self.storage.append(tweet)

    def to_file(self):
        writer = open("tweets.json", "w", encoding="utf-8")
        writer.write("{\"tags\" : ")
        writer.write(json.dumps(list(self.storage)))
        writer.write("}")
        writer.close()


class CachedTweet(object):
    def __init__(self, id, ir, cluster):
        self.id = id
        self.max_ir = ir
        self.recent_ir = ir
        self.max_cluster = cluster
        self.recent_cluster = cluster

    def update(self, ir, cluster):
        self.recent_ir = ir
        self.recent_cluster = cluster
        if ir > self.max_ir:
            self.max_ir = ir
            self.max_cluster = cluster


class Cache(object):
    def __init__(self):
        self.cache = []
        self.restore_cache("cache.json")

    def update_cache(self, new_cache):
        temp = []
        for cluster in new_cache:
            for tweet in cluster["ids"]:
                # check if tweet is already in cache
                if any(x.id == tweet for x in self.cache):
                    ctweet = [x for x in self.cache if x.id == tweet][0]
                    ctweet.update(cluster["score"], cluster["id"])
                    temp.append(ctweet)
                else:
                    temp.append(CachedTweet(tweet, cluster["score"], cluster["id"]))
        # for each tweet not in new cache
        enrich_store = ElasticHandler()
        for tweet in self.cache:
            if not any(x.id == tweet.id for x in temp):
                # store highest rating and cluster
                enrich_store.add_enrichment(tweet)
        # overwrite old cache
        self.cache = temp
        # store cache
        enrich_store.flush()
        self.store_cache()

    def store_cache(self):
        # dump cache to file
        writer = open("cache.json", "w", encoding='utf-8')
        writer.write(json.dump(self.cache))
        writer.close()

    def restore_cache(self, file):
        # read cache from file
        try:
            with open(file) as old_cache:
                self.cache = json.loads(old_cache)
        except:
            return


class DataHandler(object):
    def __init__(self):
        config = configparser.ConfigParser()
        config.read(os.path.dirname(os.path.abspath(__file__)) + "/config.ini")
        self.storage = Storage(int(config["clustering"]["max"]))
        self.cache = Cache()
        self.min = int(config["clustering"]["min"])

    def add_tweet(self, tweet):
        if "source" not in tweet:
            tweet = self.format_tweet(tweet)
        self.storage.add_tweet(self.format_tweet(tweet))

    @staticmethod
    def format_tweet(tweet):
        result = {}
        result["photos"] = []
        if tweet["entities"]["media"]:
            for media in tweet["entities"]["media"]:
                if media["type"] == "photo":
                    result["photos"].append(media["media_url_https"])
        result["urls"] = []
        if tweet["entities"]["urls"]:
            for url in tweet["entities"]["urls"]:
                result["urls"].append(url["url"])
        result["waterdepth"] = -1
        # TODO add keyword
        result["keywords"] = ["flood"]
        result["retweet"] = tweet["retweeted"]
        result["classes"] = []
        result["locations"] = []
        # in  Thu Feb 18 12:03:44 +0000 2016
        # out 2016-02-18T12:03:44.000Z
        dateparts = result["created_at"].split(" ")
        date = dateparts[5] + "-" + str('{:02d}'.format(datetime.strptime(dateparts[1],'%b').month)) + "-" + dateparts[2] + "T" + dateparts[3] + ".000Z"
        result["date"] = date
        result["text"] = tweet["text"]
        result["source"] = {}
        result["source"]["id"] = tweet["id_str"]
        result["source"]["username"] = tweet["user"]["screen_name"]
        result["source"]["userId"] = tweet["user"]["id_str"]
        result["id"] = "t-" + tweet["id_str"]
        result["labels"] = []
        return result

    def start_clustering(self):
        if len(self.storage.storage) < self.min:
            return False
        self.storage.to_file()
        # cluster storage
        handler = AlgorithmHandler()
        handler.start_algorithm()
        return True

    def process_results(self):
        # read file
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'result.json'),
                  encoding="utf8") as data_file:
            clusters = json.load(data_file)
        # store clusters
        mongo = MongoHandler()
        mongo.add_clusters(clusters)
        # process old cache and store in database
        self.cache.update_cache(clusters)
        # remove result.json
        os.remove(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'result.json'))


class AlgorithmHandler(object):
    def start_algorithm(self):
        """
        starts the algorithm
        :param source: datastream to be used
        :param frame: time frame the algorithm uses to filter
        :param loops: amount of times the algorithm is repeated after it's initial run
        :return: None
        """
        config = configparser.ConfigParser()
        config.read(os.path.dirname(os.path.abspath(__file__)) + "/config.ini")
        dataset = os.path.join(os.path.dirname(os.path.abspath(__file__)) + r"/tweets.json")
        output = os.path.join(os.path.dirname(os.path.abspath(__file__)) + r"/result.json")
        location = os.path.join(os.path.dirname(os.path.abspath(__file__)) + "/",
                                config['algorithm']['location'].replace("\"", ""))
        subprocess.Popen("python --version", stdout=subprocess.PIPE, shell=True)
        cmd = "python " + location + "main.py -in \"" + dataset + "\" -l 0 -out " + output + " -tp enrichment"
        subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        # remove tweets.json
        os.remove(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tweets.json'))

#endregion

# region DataAccessLayer
class MongoHandler(object):
    """
    class for handling communications with mongodb
    """

    def __init__(self):
        """
        constructor for MongoHandler
        :return: None
        """
        config = configparser.ConfigParser()
        config.read(os.path.dirname(os.path.abspath(__file__)) + "/config.ini")
        self.uri = "mongodb://" + config["mongodb"]["user"] + ":" + config["mongodb"]["password"] + "@" + \
                   config["mongodb"]["host"] + ":" + config["mongodb"]["port"] + "/" + config["mongodb"]["db"]

    def add_clusters(self, clusters):
        """
        adds clusters to mongodb specified in the config.ini file
        :param clusters: clusters of tweets
        :return: None
        """
        client = MongoClient(self.uri)
        db = client.clusters
        cluster_storage = db.cluster_storage
        cluster_storage.insert_many(clusters)
        client.close()


class ElasticHandler(object):
    def __init__(self):
        self.enrichments = []

    def add_enrichment(self, tweet):
        self.enrichments.append(tweet)

    def flush(self):
        pass


# endregion


if __name__ == '__main__':
    #region serversetup
    conf = {
        '/': {
            'tools.sessions.on': True,
            'tools.staticdir.root': os.path.abspath(os.getcwd()),
            'tools.encode.encoding': 'utf-8'
        }
    }
    newconf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.sessions.on': True,
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'text/plain')],
            'tools.encode.on': True,
            'tools.encode.encoding': 'utf-8'
        }
    }
    cherrypy.server.socket_host = '0.0.0.0'
    data_handler = DataHandler()
    cherrypy.tree.mount(App(), '/', conf)
    cherrypy.tree.mount(Tweet(data_handler), '/tweet', newconf)
    # cherrypy.quickstart(App(), '/', conf)
    cherrypy.engine.start()
    # cherrypy.engine.block()
    #endregion
    #region clusterloop
    # every 10 minutes start clustering
    while True:
        time.sleep(10 * 60)
        ready = data_handler.start_clustering()
        if not ready:
            continue
        # loop till clustering is done
        while True:
            if 'result.json' in os.listdir(os.path.dirname(os.path.abspath(__file__))):
                print("starting processing")
                data_handler.process_results()
                break
            else:
                print("zzzz")
                time.sleep(10)
    #endregion


