package spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

    def main(args: Array[String]): Unit = {

        // TODO 使用Spark框架来完成WordCount的统计

        // TODO 1. 获取Spark框架的环境（连接）
        // SparkContext
        // 创建Spark的基础配置
        val sparkConf = new SparkConf().setMaster("local").setAppName("Spark-WordCount")
        // 根据配置创建上下环境连接对象
        val sc = new SparkContext(sparkConf)

        // TODO 2. 对象Spark环境对数据进行处理( API )

        // 2.1 从文件中读取数据
        // scala : Source.fromFile()
        // textFile : Spark框架可以将文件中的内容一行一行的读取过来。
        val lines: RDD[String] = sc.textFile("datas/1.txt")

        // 2.2 将每一行的字符串进行分词操作：扁平化 形成一个一个的单词
        val words: RDD[String] = lines.flatMap(line=>line.split(" "))

        // 2.3 将相同的单词分在一个组中
        val group: RDD[(String, Iterable[String])] = words.groupBy(word=>word)

        // 2.4 将分组后的数据进行统计（Iterable => length）
        //group.map()
        val wordToCount: RDD[(String, Int)] = group.mapValues(list=>list.size)

        // 2.5 将统计的结果采集后，打印在控制台
        wordToCount.collect().foreach(println)


        // TODO 3. 将环境(连接)关闭掉
        sc.stop()

    }
}
