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

    ///////////////////列转行（一行变多行）//////////////////////
    //方法1：使用union
    ss.sql(
      """
        |select name, "english" as subject, english as score from tableA
        |union
        |select name, "maths" as subject, maths as score from tableA
        |union
        |select name, "music" as subject, music as score from tableA
        |""".stripMargin)
      .show()

    //方法2：使用explode函数
    ss.sql(
      """
        |select name,
        |       subject,
        |       score
        |from (
        |         select name,
        |                map("english", english, "maths", maths, "music", music) mp
        |             from tableA
        |     ) tmp
        |         lateral view explode(mp) tf as subject, score
        |""".stripMargin)
      .show()




    ///////////////////行转列（多行变一行）//////////////////////
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

    //方法1：使用 collect_list 构造map
    ss.sql(
      """
        |select name,
        |       nvl(mp['english'], 0) as english,
        |       nvl(mp['maths'], 0)   as maths,
        |       nvl(mp['music'], 0)   as music
        |from (
        |         select name,
        |                str_to_map(concat_ws(',', collect_list(concat(subject, ":", score))), ',', ':') mp
        |         from tableB
        |         group by name
        |     ) tmp
        |""".stripMargin).show()

    /*//方法2 的 中间过程
    ss.sql(
      """
        |select name,
        |       case subject when 'english' then score else 0 end as english,
        |       case subject when 'maths' then score else 0 end   as maths,
        |       case subject when 'music' then score else 0 end   as music
        |from tableB
        |""".stripMargin).show()*/
    //方法2
    ss.sql(
      """
        |select name,
        |       sum(case subject when 'english' then score else 0 end) as english,
        |       sum(case subject when 'maths' then score else 0 end)   as maths,
        |       sum(case subject when 'music' then score else 0 end)   as music
        |from tableB
        |group by name
        |""".stripMargin).show()
  }

}
