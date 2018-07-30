import org.apache.spark.sql._
import org.apache.spark.SparkContext._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 协同过滤算法-相似推荐
  *
  * 本例通过使用MovieLens的数据集,包含用户对一个电影的评分.通过训练,计算一个用户对未看过的电影的评分.
  */
object ALSTest {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def main(args: Array[String]): Unit = {
    //加载数据源,数据源是一个用户id,电影id,评分,时间戳构成的数据集合
    val ratingFile = "file:///Users/hewe/IdeaProjects/sample_scala_spark/src/main/resources/ratings.csv"
    //创建基于DataFrame的DF
    val session = SparkSession.builder().master("local").appName("myApp").getOrCreate()
    import session.implicits._
    val ratings = session.sparkContext.textFile(ratingFile).map(parseRating).toDF()
    ratings.show()

    //将数据源,按照八二分,八作为训练数据,二作为测试数据
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    val alsExplicit = new ALS().setMaxIter(5) //最大迭代次数.
      .setRegParam(0.01) //
      .setUserCol("userId") //  用户id所在列
      .setItemCol("movieId") // 电影id所在列
      .setRatingCol("rating") //评分所在列

    val model = alsExplicit.fit(training)
    val predict = model.transform(test)

    //显示部分预测结果
    predict.show()

    //1. 由于新用户,新电影,没有看过.没有交互,在训练时没有被包括进去.冷启动问题
    //2. 训练数据中存在测试数据中不包含的内容.
    //导致预测时出现NaN,也就是空的情况
    //这里使用sql去除空,避免后面对评估产生影响
    val dtable = predict.createOrReplaceTempView("tmp_predictMovie")
    val frame = session.sql("select * from tmp_predictMovie where prediction != NaN")

    //评估器.用于评估计算出来的预测数据和真实数据的平方差,来观察训练的好坏.
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
    val rmse = evaluator.evaluate(frame)

    println(s"Explicit:Root-mean-square error = $rmse")


  }

  /**
    * 解析数据集到指定的类型中
    *
    * @param str
    * @return
    */
  def parseRating(str: String): Rating = {
    val fields = str.split(",")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
}
