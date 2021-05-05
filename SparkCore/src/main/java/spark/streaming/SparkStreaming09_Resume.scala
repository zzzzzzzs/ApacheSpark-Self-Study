package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming09_Resume {

    def main(args: Array[String]): Unit = {

        // TODO 恢复数据，从检查点恢复数据，如果找不到就创建一个新的
        val ssc = StreamingContext.getActiveOrCreate("cp", ()=>{
            val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
            val ssc = new StreamingContext(sparkConf, Seconds(3))

            val lines = ssc.socketTextStream("localhost", 9999)
            val wordToOne = lines.map((_,1))

            wordToOne.print()

            ssc
        })

        ssc.checkpoint("cp")

        ssc.start()
        ssc.awaitTermination() // block 阻塞main线程
    }
}
