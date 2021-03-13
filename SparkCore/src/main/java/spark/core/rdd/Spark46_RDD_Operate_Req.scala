package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark46_RDD_Operate_Req {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)


        // TODO Spark 案例实操
        // TODO 统计出每一个省份每个广告被点击数量排行的Top3

        // 1. 读取日志数据，获取原始数据
        val logRDD = sc.textFile("input/agent.log")

        // 2. 将数据进行结构的转换，方便统计
        //  （xxx  河北省 石家庄 zs   a） => (河北省 a) => (( 河北省，a ), 1)
        val dataRDD = logRDD.map(
            log => {
                val data = log.split(" ")
                ( (data(1), data(4)), 1 )
            }
        )

        // 3. 将转换结构后的数据进行分组聚合。
        //   (( 河北省，a ), 1) => (( 河北省，a ), sum)
        val dataReduceRDD: RDD[((String, String), Int)] = dataRDD.reduceByKey(_+_)

        // 4. 将聚合后的结果进行结构的转换
        //  (( 河北省，a ), sum) => (河北省，(a , sum))
//        dataReduceRDD.map(
//            t => {
//                ( t._1._1, (t._1._2, t._2) )
//            }
//        )
        val dataMapRDD = dataReduceRDD.map{
            case ( ( prv, adv ), sum ) => {
                ( prv, (adv, sum) )
            }
        }

        // 5. 根据省份将转换结构后的数据进行分组
        // (河北省，(a , sum)) => ( 河北省, Iterator[ (a , sum), (b , sum), (c, sum) ] )
        val dataGroupRDD: RDD[(String, Iterable[(String, Int)])] = dataMapRDD.groupByKey()

        // 6. 对分组后的数据根据统计的数量进行排序（降序）
        // 7. 将结果取前三名：Top3
        val resultRDD = dataGroupRDD.mapValues(
            iter => {
                iter.toList.sortWith(
                    (left, right) => {
                        left._2 > right._2
                    }
                ).take(3)
            }
        )

        // 8. 将结果打印到控制台
        resultRDD.collect().foreach(println)

        sc.stop()
    }

}
