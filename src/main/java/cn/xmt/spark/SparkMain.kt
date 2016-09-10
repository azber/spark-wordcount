package cn.xmt.spark

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2

fun main(args: Array<String>) {
    val inputFile = args[0]
    val outputFile = args[1]

    val conf = SparkConf().setAppName("wordCount")
    val sc = JavaSparkContext(conf)

    val input = sc.textFile(inputFile)
    val words = input.flatMap { it.split(" ").iterator() }
    val counts = words.mapToPair { Tuple2(it, 1) }.reduceByKey { x, y -> x + y }
    counts.saveAsTextFile(outputFile)
}