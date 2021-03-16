package spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operate_Action5 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 行动算子 - save
        val list = List(1,2,3,4)

        val rdd = sc.makeRDD(list,2)

        rdd.saveAsTextFile("output1")
        rdd.saveAsObjectFile("output2")
        rdd.map((_,1)).saveAsSequenceFile("output3")

        sc.stop()

    }
}
