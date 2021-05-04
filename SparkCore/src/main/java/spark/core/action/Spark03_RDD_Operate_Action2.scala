package spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operate_Action2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 行动算子

        // 所谓的行动算子其实会触发作业的执行，底层调用了sc.runJob
        // 调用一次行动算子，就会指定作业一次。

        // 但是行动算子和转换算子其实并没有那么明显的界限。

        // RDD的一个方法，有可能会触发作业的执行的同时还返回新的RDD。
        val list = List(1,2,3,4)
        val rdd = sc.makeRDD(list)
        // TODO　不会执行，没有行动算子
        rdd.map((_,1)).sortByKey()

        sc.stop()

    }
}
