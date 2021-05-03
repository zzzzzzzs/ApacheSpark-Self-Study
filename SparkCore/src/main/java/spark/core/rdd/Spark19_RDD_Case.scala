package spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Case {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 小练习：将List(List(1,2),3,List(4,5))进行扁平化操作
        val list = List(
            List(1,2),3,List(4,5)
        )

        val rdd = sc.makeRDD(list)

        rdd.flatMap(
            data => {
                data match {
                    case list:List[_] => list
                    case d => List(d)
                }
            }
        ).collect.foreach(println)

        sc.stop
    }
}
