import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object Appevent {
  val IN_CONFIG_PATH = "/user/hdfs/bdm/usedcarstatics/bdm_usedcarstatics_appeventflowconfig/dt=%s"
  val IN_PATH = "/user/hdfs/bdm/flow/bdm_flow_app_event_clean/dt=%s"
  val OUT_PATH = "/user/hdfs/bdm/flow/bdm_flow_app_eventsource/dt=%s"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    // Rules,广播
    val raw_rules = sc.newAPIHadoopFile[LongWritable,Text,TextInputFormat](IN_CONFIG_PATH.format(""))
    val rules = raw_rules.map[String](getConfigs).collect()
    sc.broadcast[Array[String]](rules)
    // content
    val raws = sc.newAPIHadoopFile[LongWritable,Text,TextInputFormat](IN_PATH.format(""))
    raws.groupByKey()

  }

  def getLines(key:(LongWritable,Text)): String = {
    ""
  }

  /**
    * 处理事件流配置信息
    * @param key
    * @return
    */
  def getConfigs(key:(LongWritable,Text)): String = {
    val strings = key._2.toString().split('\001')
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
}
