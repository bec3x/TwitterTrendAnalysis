import tweepy
from configparser import ConfigParser
import json
import sys

config = ConfigParser()
config.read('../conf/exit.conf')

def WriteTextToFile(id, text):
    file = f'{config["Resources"]["input_dir"]}{id}.csv'
    with open(file, 'w+', encoding="utf-8") as tweetFile:
        flatText = text.replace('\n', ' ').replace('\r', ' ')
        tweetFile.write(flatText)

class StreamClient(tweepy.StreamingClient):
    def __init__(self, bearer_token):
        super().__init__(bearer_token=bearer_token)

    def on_connect(self):
        print("connected")

    def on_data(self, raw_data):
        data = json.loads(raw_data)
        text = data["data"]["text"]
        tweet_id = data["data"]["id"]
        if "extended_text" in data["data"]:
            text = data["data"]["extended_text"]["full_text"]
        
        if '#' in text:
            WriteTextToFile(tweet_id, text)

if __name__ == "__main__":
    consumer_key = config['TweepyAuth']['consumer_key']
    consumer_secret = config['TweepyAuth']['consumer_secret']
    access_token = config['TweepyAuth']['access_token']
    access_secret = config['TweepyAuth']['access_secret']
    bearer_token = config['TweepyAuth']['bearer_token']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    stream = StreamClient(bearer_token=bearer_token)

    rules = config['Resources']['stream_rules']
    stream_rules = rules.split(',')

    for rule in stream_rules:
        try:
            stream.add_rules(tweepy.StreamRule(value=rule))
        except:
            e, p, t = sys.exc_info()
            print(p)
            break

    print(stream.get_rules())

    stream.filter(tweet_fields=["referenced_tweets"])

