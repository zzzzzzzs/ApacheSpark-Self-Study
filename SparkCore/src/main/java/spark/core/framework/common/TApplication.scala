package spark.core.framework.common

import spark.core.framework.controller.WordCountController
import spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

    // op 是控制抽象，可以将一段逻辑传进来
    def start(master:String ="local[*]", app:String = "Application")( op : => Unit ): Unit = {
        val sparConf = new SparkConf().setMaster(master).setAppName(app)
        val sc = new SparkContext(sparConf)
        EnvUtil.put(sc)

        try {
            op
        } catch {
            case ex => println(ex.getMessage)
        }

        // TODO 关闭连接
        sc.stop()
        EnvUtil.clear()
    }
}
