#!/usr/bin/env python

import tweepy
from tweepy.auth import OAuthHandler
from tweepy.streaming import StreamListener
import socket

import settings


class MyStreamListener(StreamListener):

    def on_status(self, status):
        """on_status

        :param status:
        """
        (conn, address) = s.accept()
        print(status.text)
        conn.send(bytes(status.text, 'UTF-8'))


if __name__ == "__main__":

    # spark streamingからのsocket接続をポート5555で受け付ける
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("localhost", 5555))
    s.listen(5)

    # TwitterAPIを利用してTwiter情報を取得する
    auth = OAuthHandler(settings.consumer_key, settings.consumer_secret)
    auth.set_access_token(settings.access_token, settings.access_secret)
    api = tweepy.API(auth)
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

    # "python"という単語を含むもののみにフィルタする
    myStream.filter(track=['python'])
