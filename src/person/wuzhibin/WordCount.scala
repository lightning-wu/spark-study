package person.wuzhibin

import org.apache.spark._

object WordCount {
  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    var inputPath = "/Users/wuzhibin/code/spark_demo/data/input/word"
    var outputPath = "/Users/wuzhibin/code/spark_demo/data/output"

    if (args.length == 1) {
      masterUrl = args(0)
    } else if (args.length == 3) {
      masterUrl = args(0)
      inputPath = args(1)
      outputPath = args(2)
    }

    println(s"masterUrl:${masterUrl}, inputPath: ${inputPath}, outputPath: ${outputPath}")

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rowRdd = sc.textFile(inputPath)
    val resultRdd = rowRdd.flatMap(line => line.split(","))
        .map(word => (word, 1)).reduceByKey(_ + _)

    resultRdd.saveAsTextFile(outputPath)
  }
}
