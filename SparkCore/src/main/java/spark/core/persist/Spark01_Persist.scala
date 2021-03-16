package spark.core.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Persist {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 持久化
        // 所谓的持久化，表示将内存中的数据保存到磁盘文件中

        // Spark在执行作业时，由于RDD的血缘关系的存在，一旦出现错误，那么计算需要从头开始。
        // 这种操作体现了框架处理的容错性，但是性能会受到极大的影响。
        // 1. 希望在保证容错性的基础上，还能够提高性能？
        //    如果增加缓存操作
        // 2. 重复计算时，能够提高性能？
        //    如果增加缓存操作

        // 如果使用缓存将RDD的计算结果进行保存。那么即使RDD中不保存数据，那么缓存中有数据
        // 就可以重复使用提高效率，一旦出现错误，也可以从缓存中获取中间处理的数据，也可以继续执行。

        // 一般情况下，会将比较重要的数据和执行时间比较长，重复使用数据进行缓存。

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        val mapRDD = rdd.map(num=>{
            println("map....")
            num
        })

        // 讲RDD的计算结果缓存起来，重复使用
        // Spark中缓存处理其实也是一种持久化
        // Spark中持久化会有一个存储级别的概念，cache其实就是存在在内存中
        // 如果想要存储到其他位置，需要使用persist方法
        mapRDD.cache()
        //mapRDD.persist(StorageLevel.DISK_ONLY)

        // TODO 1. 全数据操作
        println(mapRDD.collect.mkString(","))

        println("***************************")
        // TODO 2. 奇数操作
        println(mapRDD.filter(_%2 != 0).collect.mkString(","))


        sc.stop()

    }
}
