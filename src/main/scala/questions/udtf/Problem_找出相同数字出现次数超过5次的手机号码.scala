package questions.udtf

import org.apache.spark.sql.{DataFrame, SparkSession}

//https://www.jianshu.com/p/1d9911727c37
object Problem_找出相同数字出现次数超过5次的手机号码 {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("jimmhe", "18191512076"),
      ("xiaosong", "18392988059"),
      ("jingxianghua", "181188188"),
      ("donghualing", "17191919999")
    ).toDF("name", "tel") //姓名、电话号码

    df1.createTempView("user_info")

    //todo

  }

}
