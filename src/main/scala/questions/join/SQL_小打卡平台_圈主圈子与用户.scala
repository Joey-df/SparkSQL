package questions.join

import org.apache.spark.sql.{DataFrame, SparkSession}

// 计算小打卡平台的圈主建了多少圈子，名下有多少用户
// 已知，数据如下：圈主可以理解我微信公众号的作者，圈子可以理解为微信公众号，用户可以理解为公众号的粉丝
// tb_habit 圈子表：近千万行数据
// user_habit_relation 用户与圈子关系表：数亿行数据
// 需求：请用hive sql计算出如下结果（同一个圈主名下，同一个用户加多个圈子只计算一次）：

//以下展示为什么要用left join
//select a.master_id as master_id
//        ,a.habit_id as a_habit_id
//        ,b.habit_id as b_habit_id
//        ,b.user_id as user_id
//  from
//    tb_habit a left join user_habit_relation b
//  on a.habit_id = b.habit_id
//---------------------------------
//OK
//open_id1	habit_id1	habit_id1	user_id1
//open_id1	habit_id1	habit_id1	user_id4
//open_id1	habit_id1	habit_id1	user_id3
//open_id1	habit_id2	habit_id2	user_id5
//open_id1	habit_id2	habit_id2	user_id7
//open_id1	habit_id2	habit_id2	user_id1
//open_id1	habit_id3	habit_id3	user_id2
//open_id1	habit_id3	habit_id3	user_id1
//open_id2	habit_id4	habit_id4	user_id11
//open_id2	habit_id4	habit_id4	user_id1
//open_id2	habit_id4	habit_id4	user_id12
//open_id2	habit_id5	NULL	        NULL
//open_id3	habit_id6	habit_id6	user_id17
//open_id3	habit_id7	NULL	        NULL
//Time taken: 10.573 seconds, Fetched: 14 row(s)

//本题是典型的SQL思维之join思维的典型代表题，主要采用join思维进行解题，根据题意建立映射关系图，然后选择合适的join进行求解。
//本题拓展案例：
//需求：计算平台的每一个用户发过多少朋友圈、获得多少点赞。
//两张表：用户与朋友圈文章对应表user_log、朋友圈文章与点赞的关系表log_like
//答案：
//SELECT a.uid,                                   --用户id
//       nvl(COUNT(a.log_id), 0)     AS log_cnt,  --朋友圈文章数
//       nvl(SUM(b.like_uid_cnt), 0) AS liked_cnt --点赞数
//FROM user_log a
//         LEFT JOIN
//     (
//         SELECT log_id, COUNT(like_uid) AS like_uid_cnt
//         FROM log_like
//         GROUP BY log_id
//     ) b
//     ON a.log_id = b.log_id
//GROUP BY a.uid;
object SQL_小打卡平台_圈主圈子与用户 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._
    //预期结果：
    //open_id1	3	6
    //open_id2	2	3
    //open_id3	2	1
    val df1: DataFrame = List(
      ("open_id1","habit_id1"),
      ("open_id1","habit_id2"),
      ("open_id1","habit_id3"),
      ("open_id2","habit_id4"),
      ("open_id2","habit_id5"),
      ("open_id3","habit_id6"),
      ("open_id3","habit_id7")
    ).toDF("master_id", "habit_id") //圈主id、圈子id

    df1.createTempView("tb_habit")

    val df2: DataFrame = List(
      ("habit_id1","user_id1"),
      ("habit_id1","user_id3"),
      ("habit_id1","user_id4"),
      ("habit_id3","user_id2"),
      ("habit_id3","user_id1"),
      ("habit_id2","user_id5"),
      ("habit_id2","user_id1"),
      ("habit_id2","user_id7"),
      ("habit_id4","user_id11"),
      ("habit_id4","user_id12"),
      ("habit_id4","user_id1"),
      ("habit_id6","user_id17")
    ).toDF("habit_id", "user_id") //圈子id、用户id

    df2.createTempView("user_habit_relation")

    ss.sql(
      """
        |select * from tb_habit
        |""".stripMargin).show()

    ss.sql(
      """
        |select * from user_habit_relation
        |""".stripMargin).show()

    //todo
  }

}
