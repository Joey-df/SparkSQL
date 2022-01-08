package questions.practice

/**
 * 第五题（尚硅谷第5题）
 * 需求：
 * 有日志如下，请用SQL求得所有用户和活跃用户的总数及平均年龄。
 * （活跃用户指连续两天都有访问记录的用户）
 * 日期       用户   年龄
 * +----------+-------+---+
 * | date_time|user_id|age|
 * +----------+-------+---+
 * |2019-02-12|      2| 19|
 * |2019-02-11|      1| 23|
 * |2019-02-11|      3| 39|
 * |2019-02-11|      1| 23|
 * |2019-02-11|      3| 39|
 * |2019-02-13|      1| 23|
 * |2019-02-15|      2| 19|
 * |2019-02-11|      2| 19|
 * |2019-02-11|      1| 23|
 * |2019-02-16|      2| 19|
 * +----------+-------+---+
 */
object Problem_0005 {

  def main(args: Array[String]): Unit = {

    //SELECT sum(total_user_cnt)     total_user_cnt,
    //       sum(total_user_avg_age) total_user_avg_age,
    //       sum(two_days_cnt)       two_days_cnt,
    //       sum(avg_age)            avg_age
    //FROM (SELECT 0                                             total_user_cnt,
    //             0                                             total_user_avg_age,
    //             count(*)                                   AS two_days_cnt,
    //             cast(sum(age) / count(*) AS decimal(5, 2)) AS avg_age
    //      FROM (
    //               SELECT user_id,
    //                      max(age) age
    //               FROM (
    //                        SELECT user_id,
    //                               max(age) age
    //                        FROM (
    //                                 SELECT user_id,
    //                                        age,
    //                                        date_sub(date_time, rank) flag
    //                                 FROM (SELECT date_time,
    //                                              user_id,
    //                                              max(age)                                                    age,
    //                                              row_number() over (PARTITION BY user_id ORDER BY date_time) rank
    //                                       FROM test_db.log_info
    //                                       GROUP BY date_time, user_id) t1
    //                             ) t2
    //                        GROUP BY user_id, flag
    //                        HAVING count(*) >= 2
    //                    ) t3
    //               GROUP BY user_id
    //           ) t4
    //
    //      UNION ALL
    //
    //      SELECT count(*)                                   total_user_cnt,
    //             cast(sum(age) / count(*) AS decimal(5, 2)) total_user_avg_age,
    //             0                                          two_days_cnt,
    //             0                                          avg_age
    //      FROM (
    //               SELECT user_id,
    //                      max(age) age
    //               FROM test_db.log_info
    //               GROUP BY user_id
    //           ) t5
    //     ) t6;
  }

}
