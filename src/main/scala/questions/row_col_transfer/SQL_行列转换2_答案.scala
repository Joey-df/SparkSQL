package questions.row_col_transfer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 考察函数：
 *  map函数
 *  union
 *  lateral view explode
 *
 * 数据准备
 * create table score_info (
 * name varchar(20),
 * english int,
 * maths int,
 * music int);
 * insert into score_info values
 * ("Jim",90,88,99);
 */
object SQL_行列转换2_答案 {

  def main(args: Array[String]): Unit = {


    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("Jim",90,88,99)
    ).toDF("name","english","maths","music")

    df1.createTempView("tableA")
    ss.sql(
      """
        | select * from tableA
        |""".stripMargin)
      .show()

    //todo
//    //方法1：使用union
//    ss.sql(
//      """
//        |
//        |""".stripMargin)
//      .show()
//
//    //方法2：使用explode函数
//    ss.sql(
//      """
//        |
//        |""".stripMargin)
//      .show()





    val df2: DataFrame = List(
      ("Jim","english",90),
      ("Jim","maths",88),
      ("Jim","music",99)
    ).toDF("name","subject","score")

    df2.createTempView("tableB")
    ss.sql(
      """
        | select * from tableB
        |""".stripMargin)
      .show()

    //todo
  }

}
