package questions.udtf

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 有一张hive表 info，两个字段：
 * 分别是学生姓名name(string)，
 * 学生成绩score(map<string,string>), 成绩列中key是学科名称，value是对应学科分数，
 * 请用一个hql求一下每个学生成绩最好的学科及分数、最差的学科及分数
 */
object Problem_求每个学生成绩最好的学科及分数_最差的学科及分数_平均分数 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      ("张三","Chinese:80,Math:60,English:90"),
      ("李四","Chinese:90,Math:80,English:70"),
      ("王五","Chinese:88,Math:90,English:96"),
      ("马六","Chinese:99,Math:65,English:60")

    ).toDF("name", "score_map") //姓名、成绩信息

    df1.createTempView("info")

    ss.sql(
      """
        | select * from info
        |""".stripMargin).show()

    //求平均分
    ss.sql(
      """
        |select name,
        |       avg(score) avg
        |from (
        |         select name,
        |                course,
        |                score
        |         from info lateral view posexplode(str_to_map(score_map,",",":")) tf as pos, course, score
        |     ) tmp
        |group by name
        |
        |""".stripMargin).show()

    //求每个学生最差学科与成绩、最好学科与成绩
    ss.sql(
      """
        |select name,
        |       course,
        |       score,
        |       if(asc_rn = 1, 'min', 'max') level
        |from (
        |         select name,
        |                course,
        |                score,
        |                rank() over (partition by name order by score)      as asc_rn,
        |                rank() over (partition by name order by score desc) as desc_rn
        |         from (
        |                  select name,
        |                         course,
        |                         score
        |                  from info lateral view explode(str_to_map(score_map, ",", ":")) tf as course, score
        |              ) tmp
        |     ) tt
        |where asc_rn = 1 or desc_rn = 1
        |""".stripMargin).show()


  }

}
