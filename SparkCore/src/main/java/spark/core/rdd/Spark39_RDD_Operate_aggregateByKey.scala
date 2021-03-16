package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark39_RDD_Operate_aggregateByKey {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - (KV)类型
        // 取出每个分区内相同key的最大值然后分区间相加
        val list = List(
            ("a",1),("a",2),("c",3),
            ("b",4),("c",5),("c",6)
        )
        var rdd = sc.makeRDD(list,2)
        // 0 => (a, 2)(c,3)
        // 1 => (b, 4)(c,6)
        // merge => (a,2)(c,9)(b,4)

        // （a, 5）(a,1)(a,2) => (a,5) , (c,5)
        //  (b,5)(b,4)        => (b, 5) (c,6)
        // (a, 5), (b, 5), (c, 11)
        //val value: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1) //(X)
        //val value: RDD[(String, Iterable[Int])] = rdd.groupByKey() //(x)
        // reduceByKey分区内和分区间的计算规则相同。
        //rdd.reduceByKey(_+_) // （x）

        // TODO aggregateByKey : 根据Key聚合数据, 使用函数柯里化

        //    第一个参数列表中传递的参数为 ： zeroValue （初始值）
        //         当分区内的第一个key出现时，无法执行分区内的两两计算规则
        //         所以需要准备一个初始值和第一个值进行分区内的计算。
        //    第二个参数列表中传递2个参数：
        //         seqOp ： 分区内计算规则，相同key的数据在分区内两两计算的规则
        //         combOp ：分区间计算规则，相同key的数据在分区间两两计算的规则
        val newRDD: RDD[(String, Int)] = rdd.aggregateByKey(5)(
            (x, y) => math.max(x, y),
            (x, y) => x + y
        )
        newRDD.collect.foreach(println)


        sc.stop()
    }
}
