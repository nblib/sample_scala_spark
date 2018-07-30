import org.apache.hadoop.io.Text
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.Vector

object MLlibTest {
  def main(args: Array[String]): Unit = {
    lr()
  }

  def getConfig(): String = {
    val text = new Text("nihao\001hewe\001")
    val strings = text.toString().split('\001')
    if (strings.length < 3) {
      null
    } else if (strings.length == 3) {
      strings(2) + "$"
    } else {
      val source = strings(2)
      val secondsource = strings(3)
      source + "$" + secondsource
    }
  }

  def tell: Unit = {
    println("nihao1")
  }

  def name(f: => Unit) = {
    println("exe name...")
    f
  }

  /**
    * 逻辑回归
    */
  def lr() = {
    val session = SparkSession.builder().master("local").appName("myApp").getOrCreate()
    //训练数据
    val training = session.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "d b", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop masdf", 0.0)
    )).toDF("id", "text", "label")
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val tF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)

    val pipeLine = new Pipeline().setStages(Array(tokenizer, tF, lr))

    val model = pipeLine.fit(training)

    val test = session.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark a"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    model.transform(test).
      select("id", "text", "probability", "prediction").
      collect().
      foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
  }
}
