import urllib.request
import urllib.parse
import os
import json

def main():
    # read files
    result = []
    counter = 550
    while len(result) < 5000:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'dummydata/data' + str(counter) + '.json'), encoding="utf8") as data_file:
            result += json.load(data_file)
        counter -= 1
    # for each tweet run post
    for tweet in result:
        post(tweet)
    # check iets...
    pass


def post(data):
    try:
        url = "http://localhost:8080/tweet"
        f = urllib.request.urlopen(url, ("tweet=" + urllib.parse.quote(json.dumps(data))).encode("utf-8"))
        f.close()
    except urllib.error.HTTPError as e:
        print(str("tweet=" + json.dumps(data)))

if __name__ == '__main__':
    main()
