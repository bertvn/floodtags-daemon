import json
import os
import time

from collections import deque
import cherrypy


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
        print(tweet)
        self.handler.add_tweet(tweet)


class Storage(object):
    def __init__(self):
        self.storage = deque([], 5000)

    def add_tweet(self, tweet):
        # first in first out max 5k tweets
        # https://docs.python.org/3/library/collections.html#collections.deque
        self.storage.append(tweet)


class CachedTweet(object):
    def __init__(self, id, ir, cluster):
        self.id = id
        self.max_ir = ir
        self.recent_ir = ir
        self.max_cluster = cluster
        self.recent_cluster = cluster

    def update(self,ir,cluster):
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
        self.storage = Storage()
        self.cache = Cache()
        # check if there is a cached

    def add_tweet(self, tweet):
        self.storage.add_tweet(tweet)

    def start_clustering(self):
        print("called")
        print(len(self.storage.storage))
        for tweet in self.storage.storage:
            print(tweet)
        # cluster storage
        # storage -> cache
        # process old cache and store in database
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
        time.sleep(1 * 60)
        data_handler.start_clustering()
