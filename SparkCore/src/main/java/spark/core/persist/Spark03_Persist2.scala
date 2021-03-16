package spark.core.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Persist2 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 持久化

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)
        // 设置检查点的保存路径，路径在工作中一般为分布式存储路径
        sc.setCheckpointDir("cp")

        val rdd = sc.makeRDD(List(1,2,3,4))

        val mapRDD = rdd.map(num=>{
            println("map....")
            num
        })

        // TODO 如果希望计算结果可以长久地保存，那么可以使用检查点的操作
        // 因为checkpoint会有写磁盘的操作，所以性能其实会有所影响
        // 使用检查点方法的目的，为了在出现错误的情况下，可以从指定的位置重新计算，而不需要
        // 从头计算。
        // 如果不出现错误的场合，那么一般将金融数据或者安全系数比较高的数据会保存到检查点中。

        // 所以Spark在执行检查点时会重新执行一个Job来实现功能
        // 因为执行检查点操作的等同于创建一个新的作业，所以为了提高性能，一般和缓存联合使用
        // Checkpoint directory has not been set in the SparkContext
        mapRDD.cache()
        mapRDD.checkpoint() // Job

        println(mapRDD.collect().mkString(","))
        println("*******************")
        println(mapRDD.collect().mkString(","))
        println("*******************")
        println(mapRDD.collect().mkString(","))


        sc.stop()

    }
}
