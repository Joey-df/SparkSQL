package questions.udtf

import org.apache.spark.sql.{DataFrame, SparkSession}

object SQL_lateral_view_explode_有无outer的区别 {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("张三","高级-砖石","12","爬山,跳舞,看书"),
      ("李四","中级-黄金","15","看电视,跳舞" ),
      ("王五","初级-青铜","34","游泳" )
    ).toDF("name","level","age","favs") //姓名、等级、年龄、爱好

    df1.createTempView("user_info")

    ss.sql(
      """
        | select * from user_info
        |
        |""".stripMargin)
      .show()

    ss.sql(
      """
        |select name, age, level, fav
        |from user_info lateral view explode(split(favs, ",")) tf as fav
        |""".stripMargin).show()

    // le和fa的笛卡尔积
    ss.sql(
      """
        |select name,
        |       age,
        |       level,
        |       favs,
        |       le,
        |       fa
        |from user_info
        |         lateral view explode(split(level, "-")) tf1 as le
        |         lateral view explode(split(favs, ",")) tf2 as fa
        |""".stripMargin).show()

    //不加outer，如果explode的字段为空，则整行为空
    ss.sql(
      """
        |select name,
        |       age,
        |       level,
        |       favs,
        |       col
        |from user_info
        |         lateral view explode(array()) tf1 as col
        |""".stripMargin).show()

    //加outer，如果explode的字段为空，炸裂的出字段用null填充，其他字段正常输出
    ss.sql(
      """
        |select name,
        |       age,
        |       level,
        |       favs,
        |       col
        |from user_info
        |         lateral view outer explode(array()) tf1 as col
        |""".stripMargin).show()
  }

}
