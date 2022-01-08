package questions.practice

/**
 * 第一题
 * 需求：
 * 已知一个表order，有如下字段:date_time，order_id，user_id，amount。
 * 数据样例:2020-10-10,1003003981,00000001,1000，请用sql进行统计:
 * (1)2019年每个月的订单数、用户数、总成交金额。
 * (2)2020年10月的新客数(指在2020年10月才有第一笔订单)
 */
object Problem_0001 {

  def main(args: Array[String]): Unit = {

    //   (1)
    //   SELECT t1.year_month,
    //          count(t1.order_id)         AS order_cnt,
    //          count(DISTINCT t1.user_id) AS user_cnt,
    //          sum(amount)                AS total_amount
    //   FROM (SELECT order_id,
    //                user_id,
    //                amount,
    //                date_format(date_time, 'yyyy-MM') year_month
    //         FROM test_db.order
    //         WHERE date_format(date_time, 'yyyy') = '2019'
    //        ) t1
    //   GROUP BY t1.year_month;
    //
    //

    //   (2)
    //   SELECT count(user_id)
    //   FROM test_db.order
    //   GROUP BY user_id
    //   HAVING date_format(min(date_time), 'yyyy-MM') = '2020-10';

  }

}
