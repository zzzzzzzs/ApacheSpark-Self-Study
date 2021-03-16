package spark.core.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Persist4 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 持久化

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List("Hello", "Scala", "Hello"))

        val mapRDD = rdd.map(word=>{
            println("map....")
            (word,1)
        })

        // 所有的shuffle聚合算子其实都会自动包含cache操作
        // DISK_ONLY & 检查点
        // DISK_ONLY保存在Spark集群的节点中，不利用数据共享使用
        // DISK_ONLY只能够在单个Job使用，一旦Job执行完毕，存储的数据文件会被删除
        // 检查点一般保存在分布式存储中。可以跨越多个Job，达到共享数据的目的
        // 并且可以作为数据源
//        val reduceRDD = mapRDD.reduceByKey(
//            (x, y) => {
//                println("reduce...")
//                x + y
//            }
//        )
        println(mapRDD.collect.mkString(","))
        println("*************************")
        println(mapRDD.collect.mkString(","))


        sc.stop()

    }
}
