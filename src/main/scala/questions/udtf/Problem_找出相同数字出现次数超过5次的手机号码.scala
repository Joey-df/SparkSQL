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
      ("jimmhe", "1111122222"),
      ("xiaosong", "18392988059"),
      ("jingxianghua", "181188188"),
      ("donghualing", "17191919999")
    ).toDF("name", "tel") //姓名、电话号码

    df1.createTempView("user_info")

    ss.sql(
      """
        |select name,
        |       tel
        |from (
        |         select name,
        |                tel,
        |                num,
        |                count(1) as cnt
        |         from (
        |                  select name,
        |                         tel,
        |                         num
        |                  from user_info lateral view explode(split(tel, "")) tf as num
        |              ) t0
        |         group by name, tel, num
        |         having count(1) >= 5
        |     ) t1
        |group by name, tel -- 最后一步去重，防止1111122222这种号码
        |""".stripMargin).show()

  }

}
