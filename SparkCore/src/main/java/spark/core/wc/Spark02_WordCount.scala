package spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {

    def main(args: Array[String]): Unit = {

        // TODO 使用reduceByKey操作要比groupBy， map 性能要高

        val sparkConf = new SparkConf().setMaster("local").setAppName("Spark-WordCount")

        val sc = new SparkContext(sparkConf)

        val lines: RDD[String] = sc.textFile("input/word.txt")

        val words: RDD[String] = lines.flatMap(line=>line.split(" "))

        // 将单词转换结构
        // word => (word, 1)
        val wordToOne:RDD[(String, Int)] = words.map(
            word => {
                ( word, 1 )
            }
        )

        // 相同的key放置在一起，对value进行聚合
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        wordToSum.collect().foreach(println)


        // TODO 3. 将环境(连接)关闭掉
        sc.stop()

    }
}
