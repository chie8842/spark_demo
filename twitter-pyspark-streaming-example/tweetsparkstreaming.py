#!/usr/bin/env python

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def main():
    # SparkContext、StreamingContextの初期化
    sc = SparkContext("local[2]", "TwitterStreaming")
    ssc = StreamingContext(sc, 1)

    # デモを見やすくするため、ログ出力を少なくする
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    # localhost:5555にソケット接続する
    lines = ssc.socketTextStream("localhost", 5555)

    # 取得したツイート情報を標準出力する
    lines.pprint()

    # ストリーム処理開始
    ssc.start()
    # Terminateされるまで待機
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
