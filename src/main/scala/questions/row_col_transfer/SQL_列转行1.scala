package questions.row_col_transfer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 考察函数：
 *  map函数
 *  union
 *  lateral view explode
 */
object SQL_列转行1 {

  def main(args: Array[String]): Unit = {


    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("张三",50,60,70),
      ("李四",55,66,77),
      ("王五",44,0,56)
    ).toDF("name","Math","Chinese","English")

    df1.createTempView("TEST")
    ss.sql(
      """
        | select * from TEST
        |""".stripMargin)
      .show()

    //方法1：使用union
    ss.sql(
      """
        | select name, "Math" as course, Math as score from TEST where Math > 0
        | union
        | select name, "English" as course, English as score from TEST where English > 0
        | union
        | select name, "Chinese" as course, Chinese as score from TEST where Chinese > 0
        |""".stripMargin)
      .show()

    //方法2：使用explode函数
    ss.sql(
      """
        | select name, course, score from TEST
        | lateral view explode(map("数学",Math,"英语",English,"语文",Chinese)) tf as course, score
        | where score > 0
        |""".stripMargin)
      .show()
  }

}
