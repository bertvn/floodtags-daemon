import configparser
import json
import os
import subprocess
import time
import urllib
import urllib.parse
from collections import deque
from datetime import datetime

import cherrypy
from pymongo import MongoClient


# region API
class App(object):
    """
    filler class fills the / webpage
    """

    @cherrypy.expose
    def index(self):
        """
        webpage that is shown when / is called
        :return: webpage that is to be shown
        """
        return "nothing to see here"


class Tweet(object):
    """
    Tweet API for accepting tweets
    """
    exposed = True

    def __init__(self, handler):
        """
        constructor for Tweet
        :param handler: data handler class
        :return: None
        """
        self.handler = handler

    @cherrypy.tools.accept(media='text/plain')
    def POST(self, tweet):
        """
        API for sending in tweets
        :param tweet: tweet that needs to be enriched
        :return: None
        """
        print(tweet)
        self.handler.add_tweet(json.loads(tweet))


# endregion

# region businesslayer
class Storage(object):
    """
    Storage class for received tweets
    """

    def __init__(self, maximum):
        """
        constructor for storage
        :param maximum: maximum amount tweets kept by Storage
        :return: None
        """
        self.storage = deque([], maximum)

    def add_tweet(self, tweet):
        """
        add tweet to storage, if storage is full the first tweet added will be remove to make room
        :param tweet: tweet that should be added to storage
        :return: None
        """
        # first in first out max 5k tweets (maximum)
        # https://docs.python.org/3/library/collections.html#collections.deque
        self.storage.append(tweet)

    def to_file(self):
        """
        dump storage into a json file
        :return: None
        """
        writer = open("tweets.json", "w", encoding="utf-8")
        writer.write("{\"tags\" : ")
        writer.write(json.dumps(list(self.storage)))
        writer.write("}")
        writer.close()


class CachedTweet(object):
    """
    cached tweet object
    used for storing recent importance value and clusters as well as the max
    """

    def __init__(self, id, ir, cluster, max_ir=None, max_cluster=None):
        """
        constructor for CachedTweet
        :param id: id of the tweet
        :param ir: importance rating
        :param cluster: cluster that this tweet is part of
        :return: None
        """
        self.id = id
        self.recent_ir = ir
        self.recent_cluster = cluster
        if max_ir is None:
            self.max_ir = ir
            self.max_cluster = cluster
        else:
            self.max_ir = max_ir
            self.max_cluster = max_cluster

    def update(self, ir, cluster):
        """
        update Cached tweet with a new recent importance rating and cluster
        :param ir: imporatice rating
        :param cluster: cluster that this tweet is part of
        :return: None
        """
        self.recent_ir = ir
        self.recent_cluster = cluster
        if ir > self.max_ir:
            self.max_ir = ir
            self.max_cluster = cluster

    def to_json(self):
        """
        create json representation of the object
        :return: json version
        """
        return "{\"id\" : \"" + str(self.id) + "\", \"recent_ir\" : \"" + str(
            self.recent_ir) + "\", \"recent_cluster\" : \"" + str(self.recent_cluster) + "\", \"max_ir\" : \"" + str(
            self.max_ir) + "\", \"max_cluster\" : \"" + str(self.max_cluster) + "\"}"

    def to_final(self):
        """
        creates json representation of the object, with only the max values
        :return: json version
        """
        return "{\"id\" : \"" + str(self.id) + "\", \"max_ir\" : \"" + str(
            self.max_ir) + "\", \"max_cluster\" : \"" + str(self.max_cluster) + "\"}"


class Cache(object):
    """
    cache object for storing processed tweets
    """

    def __init__(self):
        """
        constructor for cache
        if available loads existing cache
        :return: None
        """
        self.cache = []
        self.restore_cache("cache.json")

    def update_cache(self, new_cache):
        """
        store results from the algorithm in the cache and store enrichments for tweets that have left the algorithm
        :param new_cache: results from the algorithm
        :return: None
        """
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
        enrich_store = EnrichmentHandler()
        for tweet in self.cache:
            if not any(x.id == tweet.id for x in temp):
                # store highest rating and cluster
                enrich_store.add_enrichment(tweet, True)
        # overwrite old cache
        self.cache = temp
        for tweet in self.cache:
            enrich_store.add_enrichment(tweet)
        # store cache
        enrich_store.flush()
        self.store_cache()

    def store_cache(self):
        """
        dumps cache into file
        :return: None
        """
        try:
            writer = open("cache.json", "w", encoding='utf-8')
            storage = ["["]
            for tweet in self.cache:
                storage.append(tweet.to_json())
                storage.append(",")
            del storage[-1]
            storage.append("]")
            writer.write(''.join(storage))
            writer.close()
        except TypeError:
            print("storage failed")

    def restore_cache(self, file):
        """
        reads cache from file if file exists
        :param file: file containing the cache
        :return: None
        """
        try:
            with open(file) as old_cache:
                self.cache = json.loads(old_cache, object_hook=self.cache_decoder)
        except:
            return

    @staticmethod
    def cache_decoder(obj):
        return CachedTweet(obj["id"], obj["recent_ir"], obj["recent_cluster"], obj["max_ir"], obj["max_cluster"])


class DataHandler(object):
    """
    class for handling all data stored in this program
    """

    def __init__(self):
        """
        constructor for DataHandler
        can be configured through the clustering section in config.ini
        :return: None
        """
        config = configparser.ConfigParser()
        config.read(os.path.dirname(os.path.abspath(__file__)) + "/config.ini")
        self.storage = Storage(int(config["clustering"]["max"]))
        self.cache = Cache()
        self.min = int(config["clustering"]["min"])

    def add_tweet(self, tweet):
        """
        add tweet to the storage
        :param tweet: tweet that needs to be added
        :return: None
        """
        if "id_str" in tweet:
            tweet = self.format_tweet(tweet)
        self.storage.add_tweet(tweet)

    @staticmethod
    def format_tweet(tweet):
        """
        converts standard tweet to floodtags tweet
        :param tweet: tweet that needs to be formatted
        :return: floodtags tweet
        """
        result = {}
        result["photos"] = []
        if "media" in tweet["entities"]:
            for media in tweet["entities"]["media"]:
                if media["type"] == "photo":
                    result["photos"].append(media["media_url_https"])
        result["urls"] = []
        if "urls" in tweet["entities"]:
            for url in tweet["entities"]["urls"]:
                result["urls"].append(url["url"])
        result["waterdepth"] = -1
        # TODO add keyword
        result["keywords"] = tweet["dependencies"]["keywords"]
        result["retweet"] = tweet["dependencies"]["retweet"]
        result["classes"] = []
        result["locations"] = []
        # in  Thu Feb 18 12:03:44 +0000 2016
        # out 2016-02-18T12:03:44.000Z
        dateparts = tweet["created_at"].split(" ")
        date = dateparts[5] + "-" + str('{:02d}'.format(datetime.strptime(dateparts[1], '%b').month)) + "-" + dateparts[
            2] + "T" + dateparts[3] + ".000Z"
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
        """
        start clustering algorithm
        :return: boolean containing whether or not there were enough tweets to start the algorithm
        """
        if len(self.storage.storage) < self.min:
            return False
        self.storage.to_file()
        # cluster storage
        handler = AlgorithmHandler()
        handler.start_algorithm()
        return True

    def process_results(self):
        """
        read and process the results from the algorithm
        :return: None
        """
        # read file
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'result.json'),
                  encoding="utf8") as data_file:
            clusters = json.load(data_file)
        # store clusters
        mongo = MongoHandler()
        mongo.add_clusters(clusters)
        # process old cache and store in database
        self.cache.update_cache(clusters)
        # remove tweets.json
        os.remove(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tweets.json'))
        # remove result.json
        os.remove(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'result.json'))


class AlgorithmHandler(object):
    """
    class for starting the algorithm
    """

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


# endregion

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
        self.uri = ""
        if int(config["mongodb"]["use_credentials"]) == 1:
            self.uri = "mongodb://" + config["mongodb"]["user"] + ":" + config["mongodb"]["password"] + "@" + \
                       config["mongodb"]["host"] + ":" + config["mongodb"]["port"] + "/" + config["mongodb"]["db"]
        else:
            self.uri = "mongodb://" + config["mongodb"]["host"] + ":" + config["mongodb"]["port"] + "/" + \
                       config["mongodb"]["db"]

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


class EnrichmentHandler(object):
    """
    class for handling communication with the enrichment updater
    """

    def __init__(self):
        """
        constructor for ElasticHandler
        :return: None
        """
        self.enrichments = []
        config = configparser.ConfigParser()
        config.read(os.path.dirname(os.path.abspath(__file__)) + "/config.ini")
        self.url = "http://" + config["enrichment"]["host"] + ":" + config["enrichment"]["port"] + "/" + \
                   config["enrichment"]["path"]

    def add_enrichment(self, tweet, final=False):
        """
        add enrichment that needs to be processed
        :param tweet: enrichment
        :param final: should only max be pushed
        :return: None
        """
        if final:
            enrichment = tweet.to_final()
        else:
            enrichment = tweet.to_json()
        self.enrichments.append(enrichment)

    def flush(self):
        """
        send enrichments to the enrichment updater
        :return:
        """
        for enrichment in self.enrichments:
            self.post(enrichment)

    def post(self, data):
        """
        post data to server
        :param data: data that needs to be send
        :return: None
        """
        try:
            f = urllib.request.urlopen(self.url, ("enrichment=" + urllib.parse.quote(data)).encode("utf-8"))
            f.close()
        except urllib.error.HTTPError as e:
            print(str("enrichment=" + data))


# endregion

def fill_data():
    result = []
    counter = 550
    while len(result) < 500:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               '../tests/TweetTest/dummydata/data' + str(counter) + '.json'),
                  encoding="utf8") as data_file:
            result += json.load(data_file)
        counter -= 1

    return result


def main():
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
    # add web apps that need to be run
    cherrypy.tree.mount(App(), '/', conf)
    cherrypy.tree.mount(Tweet(data_handler), '/tweet', newconf)

    cherrypy.engine.start()
    # cherrypy.engine.block()

    """ test to see how it react when filled
    tweets = fill_data()
    for tweet in tweets:
        data_handler.add_tweet(tweet)
    """

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


if __name__ == '__main__':
    main()
