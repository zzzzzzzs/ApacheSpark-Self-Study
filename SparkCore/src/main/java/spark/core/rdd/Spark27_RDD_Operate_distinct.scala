package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark27_RDD_Operate_distinct {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        /* TODO Spark - 转换算子 - distinct (去重)
            不用distinct算子可以用set集合去重，但是性能不高，单点去重可能出现问题，所以在大数据领域使用distinct去重
            distinct 存在shuffle的过程，底层用了 reduceByKey
         */
        val list = List(1,2,3,4,1,2,3,4)

        val rdd = sc.makeRDD(list,2)

        //rdd.distinct().collect().foreach(println)
        // (1,1)
        // => (1, null),(1, null)
        // => 1, (null, null) => null => (1, null)
        // => (1)
        // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)

        rdd.saveAsTextFile("output")
        // distinct可以将数据打乱重新组合，所以存在shuffle操作
        rdd.distinct().saveAsTextFile("output1")
        // 如果数据存在shuffle的过程，那么就可以改变分区
        rdd.distinct(3).saveAsTextFile("output2")


        sc.stop
    }
}
