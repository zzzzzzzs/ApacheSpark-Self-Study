package spark.core.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Persist3 {

    def main(args: Array[String]): Unit = {

        // TODO Spark - 持久化

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        sc.setCheckpointDir("cp")

        val rdd = sc.makeRDD(List(1,2,3,4))

        val mapRDD1 = rdd.map(num=>{
            //println("map....")
            num
        })
        val mapRDD2 = mapRDD1.map(num=>{
            //println("map....")
            num
        })
        val mapRDD3 = mapRDD2.map(num=>{
            //println("map....")
            num
        })


        // TODO cache方法会在血缘关系中增加依赖
        //   因为cache方法会缓存数据，但是数据是不安全。可能数据会丢失
        //   为了避免数据统计结果有问题，不能让数据丢失，所以需要重新计算。所以血缘关闭不能丢失
        // TODO checkpoint方法会切断血缘关系
        //   因为在生产环境中，检查点的数据保存在分布式存储中，所以相对来讲，数据会安全很多。
        //   不用考虑数据丢失的问题，就可以将检查点的数据当成数据源，所以可以重新计算血缘关系。
        mapRDD3.checkpoint()
        println(mapRDD3.toDebugString)
        mapRDD3.collect().mkString(",")
        println("***************************")
        println(mapRDD3.toDebugString)


        sc.stop()

    }
}
