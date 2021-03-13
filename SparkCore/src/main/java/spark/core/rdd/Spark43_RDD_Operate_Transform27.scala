package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark43_RDD_Operate_Transform27 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - (KV)类型
        val list = List(
            ("a", 1), ("c",2), ("b", 3)
        )

        val list1 = List(
            (new User(),1), (new User(),2), (new User(),3)
        )

        val rdd = sc.makeRDD(list)
        val rdd1 = sc.makeRDD(list1)

        // sortBy : RangePartitioner
        // sortBy底层调用的其实就是sortByKey
        //rdd.sortBy()
        // 因为底层需要排序，那么数据就会被打乱重新组合，所以有shuffle操作
        //val sortRDD: RDD[(String, Int)] = rdd.sortByKey(false, 2)

        // 自定义的key的数据如果想要排序，需要混入Ordered特质，并重写其中用于比较的方法
        //rdd1.sortByKey(true).collect.foreach(println)
        sc.stop()
    }
//    class User extends Ordered[User] {
//        override def compare(that: User): Int = ???
//    }
}
