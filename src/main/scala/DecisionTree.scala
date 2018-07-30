import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/*
  决策树算法- 通过使用决策树,根据鸟类的特征进行分类
 */
object DecisionTree {
  case class Iris(features: org.apache.spark.ml.linalg.Vector, label: String)
  def main(args: Array[String]): Unit = {
    val logFile = "file:///Users/hewe/IdeaProjects/sample_scala_spark/src/main/resources/iris.txt"
    val session =SparkSession.builder().master("local").appName("myApp").getOrCreate()
    import session.implicits._
    //按照逗号分隔每一行的数据,前四个数据为鸟类的特征,最后一个为分类
    val unit = session.sparkContext.textFile(logFile).map[Array[String]](_.split(","))
      .map((p: Array[String]) => Iris(Vectors.dense(p(0).toDouble,p(1).toDouble,p(2).toDouble, p(3).toDouble), p(4).toString()))
    //将RDD转化为DF
    val frame = session.createDataFrame(unit)
    frame.createOrReplaceTempView("fat")
    //打印部分数据
    val df= session.sql("select * from fat")
    df.map(t => t(1)+":"+t(0)).collect().foreach(println)

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol(
      "indexedLabel").fit(df)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures")
      .setMaxCategories(4).fit(df)
    val labelConverter = new IndexToString().setInputCol("prediction")
      .setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val Array(trainingData, testData) = frame.randomSplit(Array(0.7, 0.3))
    val dtClassifier = new DecisionTreeClassifier().setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
    val pipelinedClassifier = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dtClassifier, labelConverter))
    val modelClassifier = pipelinedClassifier.fit(trainingData)

    val predictionsClassifier = modelClassifier.transform(testData)
    predictionsClassifier.select("predictedLabel", "label", "features").show(20)
  }
}
