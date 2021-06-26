from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer

consumer_key = 'A' 
consumer_secret = 'B'
access_token = 'Y'
access_token_secret = 'X'

producer = KafkaProducer(bootstrap_servers='localhost:9092') 

topic_name = 'a4'


class twitterAuth():
    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth

class TwitterStreamer():
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS() 
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter(track=["#CovidVaccine", "#COVID19"], stall_warnings=True, languages= ["en"])


class ListenerTS(StreamListener):
    def on_data(self, raw_data):
            producer.send(topic_name, str.encode(raw_data))
            return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()
