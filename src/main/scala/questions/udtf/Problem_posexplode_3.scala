package questions.udtf

import org.apache.spark.sql.{DataFrame, SparkSession}

//原始数据：
//"A","1,2,3","a,b,c"
//"B","10,20,30","aa,bb,cc"
//目标结果：
//+----+----+----+
//|name|val1|val2|
//+----+----+----+
//|   A|   1|   a|
//|   A|   2|   b|
//|   A|   3|   c|
//|   B|  10|  aa|
//|   B|  20|  bb|
//|   B|  30|  cc|
//+----+----+----+
object Problem_posexplode_3 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("A","1,2,3","a,b,c"),
      ("B","10,20,30","aa,bb,cc")
    ).toDF("name", "col_num", "col_char") //用户姓名、数字集合、字母集合

    df1.createTempView("info")

    ss.sql(
      """
        |select name,
        |       val1,
        |       val2
        |from info
        |         lateral view posexplode(split(col_num, ",")) tf1 as pos1, val1
        |         lateral view posexplode(split(col_char, ",")) tf2 as pos2, val2
        |where pos1 = pos2
        |""".stripMargin).show()
  }

}
