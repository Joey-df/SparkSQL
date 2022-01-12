package questions.row_col_transfer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 考察知识点：case when的用法 / if函数的用法
 *
 *
 * -- 创建 course 表
 * create table course (
 * id varchar(20),
 * teacher_id varchar(20),
 * week_day varchar(20),
 * has_course varchar(20)
 * );
 * insert into course value
 * (1,1,2,"Yes"),
 * (2,1,3,"Yes"),
 * (3,2,1,"Yes"),
 * (4,3,2,"Yes"),
 * (5,1,2,"Yes");
 */
object SQL_行转列2_ans {

  def main(args: Array[String]): Unit = {


    val ss = SparkSession.builder()
      .master("local")
      .appName("test0")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      (1,1,2,"Yes"),
      (2,1,3,"Yes"),
      (3,2,1,"Yes"),
      (4,3,2,"Yes"),
      (5,1,2,"Yes")
    ).toDF("id","teacher_id","week_day","has_course")

    df1.createTempView("course")
    ss.sql(
      """
        | select * from course
        |""".stripMargin)
      .show()

    //TODO
  }

}
