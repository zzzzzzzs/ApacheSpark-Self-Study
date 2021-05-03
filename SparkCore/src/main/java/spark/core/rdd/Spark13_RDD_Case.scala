package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Case {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 小练习：从服务器日志数据apache.log中获取用户请求URL资源路径
        val rdd: RDD[String] = sc.textFile("SparkCore/datas/apache.log",2)

        val newRDD = rdd.map(
            line => {
                // 将每一行数据进行分解
                val datas: Array[String] = line.split(" ")
                (datas(3), datas(6))
            }
        )
//        newRDD//.filter(_._1.startsWith("17/05/2015"))
//                        .saveAsTextFile("SparkCore/output")
        newRDD.collect().foreach(println)

        sc.stop
    }
}
