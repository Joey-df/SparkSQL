package questions.practice

/**
 * 第四题（尚硅谷第4题）
 * 需求：
 * 表user(user_id，name，age)记录用户信息，
 * 表view_record(user_id，movie_name)记录用户观影信息，
 * 请根据年龄段（每10岁为一个年龄段，70以上的单独作为一个年龄段）观看电影的次数进行排序？
 */
object Problem_0004 {

  def main(args: Array[String]): Unit = {

    //  SELECT t2.age_group,
    //         sum(t1.cnt) as view_cnt
    //  FROM (SELECT user_id,
    //               count(*) cnt
    //        FROM test_db.view_record
    //        GROUP BY user_id) t1
    //           JOIN
    //       (SELECT user_id,
    //               CASE
    //                   WHEN age <= 10 AND age > 0 THEN '0-10'
    //                   WHEN age <= 20 AND age > 10 THEN '10-20'
    //                   WHEN age > 20 AND age <= 30 THEN '20-30'
    //                   WHEN age > 30 AND age <= 40 THEN '30-40'
    //                   WHEN age > 40 AND age <= 50 THEN '40-50'
    //                   WHEN age > 50 AND age <= 60 THEN '50-60'
    //                   WHEN age > 60 AND age <= 70 THEN '60-70'
    //                   ELSE '70以上' END as age_group
    //        FROM test_db.user) t2 ON t1.user_id = t2.user_id
    //  GROUP BY t2.age_group
    //  ORDER BY t2.age_group;
  }

}
