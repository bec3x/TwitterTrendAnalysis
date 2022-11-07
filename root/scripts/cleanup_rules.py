import tweepy
from configparser import ConfigParser

config = ConfigParser()
config.read('../conf/exit.conf')

class StreamClient(tweepy.StreamingClient):
    def __init__(self, bearer_token):
        super().__init__(bearer_token=bearer_token)

    def on_connect(self):
        print("connected")

if __name__ == '__main__':
    consumer_key = config['TweepyAuth']['consumer_key']
    consumer_secret = config['TweepyAuth']['consumer_secret']
    access_token = config['TweepyAuth']['access_token']
    access_secret = config['TweepyAuth']['access_secret']
    bearer_token = config['TweepyAuth']['bearer_token']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    stream = StreamClient(bearer_token=bearer_token)

    rules = stream.get_rules()
    
    try:
        for rule in rules:
            stream.delete_rules(rule.id)
    except:
        pass

    print(stream.get_rules())
