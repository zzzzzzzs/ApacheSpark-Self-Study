package spark.core.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Persist1 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 持久化

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        val mapRDD = rdd.map(num=>{
            println("map....")
            num
        })

        // cache可以改变血缘关系，当执行出现错误的情况下，可以根据血缘从缓存中获取数据
        // 一般情况下，可以将数据计算结果保存到cache中，但是如果想要安全的，长久地保存起来
        // 那么我们一般是保存到分布式存储中HDFS
        mapRDD.cache()
        //mapRDD.persist(StorageLevel.DISK_ONLY)

        println(mapRDD.toDebugString)
        println(mapRDD.collect.mkString(","))
        println("***************************")
        println(mapRDD.toDebugString)

        sc.stop()

    }
}
