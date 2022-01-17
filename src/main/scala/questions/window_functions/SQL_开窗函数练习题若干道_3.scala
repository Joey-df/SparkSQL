package questions.window_functions

import org.apache.spark.sql.{DataFrame, SparkSession}

//原始数据(学生成绩信息)表score_info
//三个字段 name、subject、score
//1、每门学科学生成绩排名(是否并列排名、空位排名三种实现)
//2、每门学科成绩排名top 3的学生
object SQL_开窗函数练习题若干道_3 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("孙悟空","语文",87),
      ("孙悟空","数学",95),
      ("孙悟空","英语",68),
      ("大海","语文",94),
      ("大海","数学",56),
      ("大海","英语",84),
      ("宋宋","语文",64),
      ("宋宋","数学",86),
      ("宋宋","英语",84),
      ("婷婷","语文",65),
      ("婷婷","数学",85),
      ("婷婷","英语",78)
    ).toDF("name","subject","score")

    df1.createTempView("score_info")

    ss.sql(
      """
        |select * from score_info
        |""".stripMargin).show()

    //1、每门学科学生成绩排名(是否并列排名、空位排名三种实现)
    ss.sql(
      """
        |select name,
        |       subject,
        |       score,
        |       row_number() over (partition by subject order by score desc) as rn,
        |       rank() over (partition by subject order by score desc)       as rank,
        |       dense_rank() over (partition by subject order by score desc) as d_rank
        |from score_info
        |""".stripMargin).show()

    //2、每门学科成绩排名top 3的学生
    ss.sql(
      """
        |select name,
        |       subject,
        |       score,
        |       rn,
        |       rank,
        |       d_rank
        |from (
        |         select name,
        |                subject,
        |                score,
        |                row_number() over (partition by subject order by score desc) as rn,
        |                rank() over (partition by subject order by score desc)       as rank,
        |                dense_rank() over (partition by subject order by score desc) as d_rank
        |         from score_info
        |     ) tmp
        |where rank <= 3
        |""".stripMargin).show()
  }

}


