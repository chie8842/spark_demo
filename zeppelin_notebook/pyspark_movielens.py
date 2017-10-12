from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.evaluation import RegressionEvaluator

# 本来は訓練データと評価データを分けるべきだが、
# MatrixFactorizationモデルの場合、分けることが難しいので、
# 訓練データと評価データは同じものを使用している。

# データの読み込み
ratings_csv = "s3://chie8842_pycon2017/data/movielens/ratings.csv"
ratings = spark.read.csv(ratings_csv, header="true")

# モデル学習のインプットの型はInt型なので、
# String型をInt型の一意なIDに変換してくれるStirngIndexerを使う。
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index")
            .fit(ratings) for column in ["userId", "movieId", "rating"] ]

# ALSのパラメータ設定
als = ALS(
    rank=40,
    maxIter=10,
    seed=0,
    userCol="userId_index",
    itemCol="movieId_index",
    ratingCol="rating_index")

indexers.append(als)

# パイプラインで処理を実行する
pipeline = Pipeline(stages=indexers)

# モデルの学習
model = pipeline.fit(ratings)

#  モデルによるrating値の予測
predictions = model.transform(ratings)

# rmseによるモデルの評価
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating_index", 
                                predictionCol="prediction")

rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

## Root-mean-square error = 0.765455168552

from pyspark.sql.functions import split, explode

# 全ユーザに対して予測値の高いmovieを10個レコメンドする
pre_predicted = (model
                 .stages[-1]
                 .recommendForAllUsers(10)
                 .select(
                     "userId_index"
                     ,explode("recommendations").alias("exploded")))
predicted = (pre_predicted
             .select("userId_index"
                     , pre_predicted.exploded.movieId_index.alias("movieId_index")
                     , pre_predicted.exploded.rating.alias("rating")))


predicted.show(truncate=False)

# +------------+-------------+---------+
# |userId_index|movieId_index|rating   |
# +------------+-------------+---------+
# |471         |1335         |2.5067835|
# |471         |582          |2.2069256|
# |471         |1965         |2.0647614|
# |471         |2855         |2.0545995|
# |471         |1522         |2.0078022|
# |471         |3009         |1.9890733|
# |471         |632          |1.9824231|
# |471         |810          |1.9466014|
# |471         |2920         |1.929863 |
# |471         |953          |1.9287404|
# |463         |315          |5.3014526|
# |463         |899          |4.1537457|
# |463         |1082         |3.9275477|
# |463         |3180         |3.8570085|
# |463         |452          |3.7146056|



