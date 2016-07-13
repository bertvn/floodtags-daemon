import configparser
import json
import os
import subprocess
import time
import urllib.parse
from collections import deque

import cherrypy
from pymongo import MongoClient


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

    def POST(self, tweet):
        # print(tweet)
        tweet = urllib.parse.unquote(tweet)
        print(tweet)
        self.handler.add_tweet(json.loads(tweet))


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
        # for each tweet not in new cache
        # store highest rating and cluster
        # overwrite old cache
        # store cache
        pass

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
        self.storage = Storage(config["clustering"]["max"])
        self.cache = Cache()
        self.min = config["clustering"]["min"]
        # check if there is a cached

    def add_tweet(self, tweet):
        self.storage.add_tweet(tweet)

    def start_clustering(self):
        if len(self.storage.storage) < self.min:
            return False
        self.storage.to_file()
        # cluster storage
        handler = AlgorithmHandler()
        handler.start_algorithm()
        return True

    def process_results(self):
        # storage -> cache
        # process old cache and store in database
        pass

    def read_result(self):
        pass


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


class MongoHandler(object):
    # mongodb://bertvn:tracer@ds013320.mlab.com:13320/clusters
    def __init__(self):
        config = configparser.ConfigParser()
        config.read(os.path.dirname(os.path.abspath(__file__)) + "/config.ini")
        self.uri = "mongodb://" + config["mongodb"]["user"] + ":" + config["mongodb"]["password"] + "@" + \
                   config["mongodb"]["host"] + ":" + config["mongodb"]["port"] + "/" + config["mongodb"]["db"]

    def add_clusters(self, clusters):
        client = MongoClient(self.uri)
        db = client.clusters
        cluster_storage = db.cluster_storage
        cluster_storage.insert_many(clusters)
        client.close()


class ElasticHandler(object):
    pass


if __name__ == '__main__':
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
    print("runt dit nog gewoon of is het hier al afgelopen")

    # every 10 minutes start clustering
    while True:
        time.sleep(10 * 60)
        ready = data_handler.start_clustering()
        if not ready:
            continue
        # loop till clustering is done
        while True:
            if 'result.json' in os.listdir(os.path.dirname(os.path.abspath(__file__))):
                data_handler.process_results()
                break
            else:
                time.sleep(10)
