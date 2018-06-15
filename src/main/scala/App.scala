import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON




object App {
  def main(args: Array[String]): Unit = {
    val logFile = "file:///Users/hewe/Soft/spark/README.md"
    val jsonFile = "file:///Users/hewe/IdeaProjects/sample_scala_spark/src/main/resources/person.json"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    jsonReader(sc,jsonFile)
    //keyPair(sc, logFile)
    //calSpecialWordCount(sc,logFile)
    //calWordCount(sc,logFile);

    Thread.sleep(50000)
  }

  def jsonReader(sc: SparkContext, filePath: String): Unit = {

    val jsonData = sc.textFile(filePath)
    jsonData.map(s => JSON.parseFull(s)).foreach(f => f  match {
      case Some(map: Map[String, Any]) => println(map)
      case None => println("Parsing failed")
      case other => println("unkown: ",other)
    })

  }

  /**
    * 键值对
    * @param sc
    * @param filePath
    */
  def keyPair(sc: SparkContext, filePath: String): Unit = {
    val lines = sc.textFile(filePath)
    val pairs = lines.flatMap(line => line.split(" ")) //
      .map(word => (word, 1))
    pairs.reduceByKey((a,b) => a+b).foreach(f => println("data: ",f))
  }

  /**
    * 计算指定的单词数量
    * @param sc
    * @param filePath
    */
  def calSpecialWordCount(sc: SparkContext, filePath: String): Unit = {
    val lines = sc.textFile(filePath)
    val count = lines.filter(line => line.contains("Spark")).count()
    println(count)
  }


  /**
    * 计算总字数
    *
    * @param sc
    * @param filePath
    */
  def calTotalWords(sc: SparkContext, filePath: String): Unit = {
    val lines = sc.textFile(filePath)
    val lineLengths = lines.map(s => s.length)
    val totals = lineLengths.reduce((a, b) => a + b)
    printf("totals words count: %d \n", totals)

  }

  /**
    * 计算统计包含word的行数
    *
    * @param sc
    * @param filePath
    */
  def calWordCount(sc: SparkContext, filePath: String): Unit = {

    val logData = sc.textFile(filePath, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
