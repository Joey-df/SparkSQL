package questions.udtf

import org.apache.spark.sql.{DataFrame, SparkSession}

object Problem_posexplode_2_计算指定时间区间内有多少人入住_ans {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      (7,2004,"2021-03-05","2021-03-07"),
      (23,2010,"2021-03-05","2021-03-06"),
      (7,1003,"2021-03-07","2021-03-08"),
      (8,2014,"2021-03-07","2021-03-08"),
      (14,3001,"2021-03-07","2021-03-10"),
      (18,3002,"2021-03-08","2021-03-10"),
      (23,3020,"2021-03-08","2021-03-09"),
      (25,2006,"2021-03-09","2021-03-12")

    ).toDF("user_id", "room_code", "check_date", "leave_date") //用户ID、房间号、入住日期、离店日期

    df1.createTempView("hotel_info")

    //方法1：使用space
    ss.sql(
      """
        |SELECT start_dd,
        |       end_dd,
        |       count(1) as num -- 入住人数
        |from (
        |         SELECT user_id,    --用户id
        |                check_date, --入店时间
        |                leave_date, --离店时间
        |                date_add(check_date, pos)     start_dd,
        |                date_add(check_date, pos + 1) end_dd,
        |                pos,
        |                val
        |         FROM hotel_info
        |                  lateral view
        |                      posexplode(split(space(datediff(leave_date, check_date)), ' ')) tf AS pos, val -- val实际上是空格
        |         where date_add(check_date, pos) < leave_date -- 注意这个过滤条件
        |     ) tmp
        |group BY start_dd, end_dd
        |""".stripMargin).show()

    //方法2：使用repeat
    ss.sql(
      """
        |SELECT start_dd,
        |       end_dd,
        |       count(1) as num
        |from (
        |         SELECT user_id,    --用户id
        |                check_date, --入店时间
        |                leave_date, --离店时间
        |                date_add(check_date, pos)     start_dd,
        |                date_add(check_date, pos + 1) end_dd,
        |                pos,
        |                val
        |         FROM hotel_info
        |                  lateral view
        |                      posexplode(split(repeat("A-", datediff(leave_date, check_date)), '-')) tf AS pos, val -- val实际上是A
        |         where val = "A" -- 注意这个过滤条件
        |     ) tmp
        |group BY start_dd, end_dd
        |""".stripMargin).show()

  }
}
