package questions.practice2

import org.apache.spark.sql.{DataFrame, SparkSession}

object Problem_0006_三级地区表转换 {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val df1: DataFrame = List(
      (1,"北京市",0),
      (2,"山东省",0),
      (3,"昌平区",1),
      (4,"海淀区",1),
      (5,"沙闸镇",3),
      (6,"马池口镇",3),
      (7,"中关村",4),
      (8,"上地",4),
      (9,"烟台市",2),
      (10,"青岛市",2),
      (11,"五通桥区",9),
      (12,"马边区",9),
      (13,"定文镇",10),
      (14,"罗成镇",10)
    ).toDF("id", "name", "parent_id")

    df1.createTempView("area_info")

    ss.sql(
      """
        |select * from area_info
        |""".stripMargin).show()

    ss.sql(
      """
        |select faddr.name as first,
        |       saddr.name as sencond,
        |       taddr.name as third
        |from area_info faddr
        |         join (SELECT id, name, parent_id from area_info) saddr on faddr.id = saddr.parent_id
        |         join (SELECT id, name, parent_id from area_info) taddr on saddr.id = taddr.parent_id
        |""".stripMargin).show()
  }
}
