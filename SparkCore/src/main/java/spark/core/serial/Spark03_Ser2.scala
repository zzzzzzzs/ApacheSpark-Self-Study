package spark.core.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Ser2 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)
        // TODO Spark - 序列化
        //3.创建一个RDD
        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

        //3.1创建一个Search对象
        val search = new Search("hello")

        //3.2 函数传递，打印：ERROR Task not serializable
        //search.getMatch1(rdd).collect().foreach(println)

        //3.3 属性传递，打印：ERROR Task not serializable
       search.getMatch2(rdd).collect().foreach(println)


        sc.stop()
    }
    case class Search(query:String)  {

        def isMatch(s: String): Boolean = {
            s.contains(query)
        }

        // 函数序列化案例
        def getMatch1 (rdd: RDD[String]): RDD[String] = {
            //rdd.filter(this.isMatch)
            rdd.filter(isMatch)
        }

        // 属性序列化案例
        def getMatch2(rdd: RDD[String]): RDD[String] = {
            //rdd.filter(x => x.contains(this.query))
            rdd.filter(x => x.contains(query))
//            val q = query
//            rdd.filter(x => x.contains(q))
        }
    }
}
