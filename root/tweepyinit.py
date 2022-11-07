import tweepy
import webbrowser
from configparser import ConfigParser


if __name__ == '__main__':
    config = ConfigParser()
    config.read("./conf/exit.conf")

    consumer_key = config['TweepyAuth']['consumer_key']
    consumer_secret = config['TweepyAuth']['consumer_secret']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

    redirect_url = auth.get_authorization_url()
    webbrowser.open(redirect_url)

    user_pin = input('What is the pin: ')

    auth.get_access_token(user_pin)
    print(auth.access_token)
    print(auth.access_token_secret)
