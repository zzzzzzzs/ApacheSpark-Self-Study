package spark.core.serial

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Ser {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)
        // TODO Spark - 序列化

        val list = List(1,2,3,4)
        val rdd = sc.makeRDD(list)

        // 下面的代码是在Driver端执行
        val i = 20

        rdd.foreach(
            num => {
                // 下面的代码是在Executor端执行的
                // 那么需要将变量i包含到当前匿名函数的内部，改变变量的生命周期
                // 所以存在闭包。
                // 这里之所以会使用闭包，Driver会将变量i传递给Executor
                // 这个变量i就是通过闭包 检测出来的。
                // 所以Spark框架在执行作业前，必须进行闭包检测功能，判断是否序列化，如果没有序列化就不执行
                println( num + i )
            }
        )

        sc.stop()
    }
}
