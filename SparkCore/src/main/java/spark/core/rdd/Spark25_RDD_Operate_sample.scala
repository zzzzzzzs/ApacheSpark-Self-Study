package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark25_RDD_Operate_sample {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO Spark - 转换算子 - sample (采样，抽取)
        // 从需要处理的数据进行抽取，采样，进行数据分析
        // 对每一个需要处理的数据进行抽取操作。
        // 1. 从数据集中抽取多少的数据？
        // 2. 抽取的数据是否有规律?
        // 3. 抽取完数据是否放回到数据集中 （抽取方式）
        //    3.1 抽取放回
        //    3.2 抽取不放回
        val list = List(1,2,3,4,5,6,7,8)

        val rdd: RDD[Int] = sc.makeRDD(list)

        // sample算子有3个参数：
        // 1.withReplacement : 抽取数据后，是否放回，true表示放回，false表示不放回
        // 2.fraction : 抽取不放回的场景下，表示每条数据的抽取几率 ：0  - 1
        //              这里的数值表示的是抽取的几率，而不是抽取数据的比例
        //              抽取放回的场景下，表示每条数据的抽取次数（预期）,取值大于等于1
        // 3.seed : 抽取不放回的场景下，抽取数据的随机数种子, 用于打分。
        //          抽取放回的场景下，抽取数据的随机数种子, 用于次数的计算。
        //val newRDD: RDD[Int] = rdd.sample(false, 0.5, 100)
        val newRDD: RDD[Int] = rdd.sample(true, 3)

        // 随机数不随机，随机数的底层使用的是随机算法
        // Hash算法：参数（IN） => 算法（Hash） => 结果（Out）
        // 随机数的种子（第一个数）如果确定，那么随机数就是确定的。

        newRDD.collect().foreach(println)


        sc.stop
    }
}
