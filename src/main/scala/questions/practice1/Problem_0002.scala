package questions.practice1

/**
 * 第二题（尚硅谷第2题）
 * 需求：
 * 有50W个店铺，每个顾客访客访问任何一个店铺的任何一个商品时都会产生一条访问日志，
 * 访问日志存储的表名为 user_visit，
 * 访客的用户id为user_id，被访问的店铺名称为shop_name，
 *
 * 请统计：
 * 1）每个店铺的UV（访客数）
 * 2）每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数
 */
//https://www.cnblogs.com/shui68home/p/13567429.html
object Problem_0002 {

  def main(args: Array[String]): Unit = {

    //   （1）
    //   SELECT shop_name,
    //          count(*) uv
    //   FROM (
    //            SELECT user_id,
    //                   shop_name
    //            FROM test_db.user_visit
    //            GROUP BY user_id, shop_name
    //        ) t
    //   GROUP BY shop_name as t;
    // -- 还有一种是使用count(distinct)，效率没有这种group by + count高


    //   （2）
    //   SELECT t2.shop_name,
    //          t2.user_id,
    //          t2.cnt
    //   FROM (
    //            SELECT t1.*,
    //                   row_number() over (partition BY t1.shop_name ORDER BY t1.cnt DESC) rank
    //            FROM (SELECT user_id,
    //                         shop_name,
    //                         count(*) AS cnt
    //                  FROM test_db.user_visit
    //                  GROUP BY user_id, shop_name) t1
    //        ) t2
    //   WHERE rank < 4;

  }

}
