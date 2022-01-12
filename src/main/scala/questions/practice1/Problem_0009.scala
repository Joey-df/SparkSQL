package questions.practice1

/**
 * 第九题（尚硅谷第8题）
 *
 * 需求：
 * 有一个线上服务器访问日志表 log_info 格式如下（用sql答题）
 * 时间              接口            IP
 * +----------------------------------------+------------+
 * | date_time      |interface           |ip          |
 * +-------------------+--------------------+------------+
 * |2016-11-09 15:22:05|/request/user/logout| 110.32.5.23|
 * |2020-09-28 14:23:1 |/api_v1/user/detail | 57.2.1.16  |
 * |2020-09-28 14:59:40|/api_v2/read/buy    | 172.6.5.166|
 * +-------------------+--------------------+------------+
 * 求2020年9月28号下午14点（14-15点），访问/api_v1/user/detail接口的top10的ip地址
 */
object Problem_0009 {

  def main(args: Array[String]): Unit = {

    // SELECT ip,
    //        count(*) AS count
    // FROM test_db.log_info
    // WHERE date_format(date_time, 'yyyy-MM-dd HH') >= '2020-09-28 14'
    //   AND date_format(date_time, 'yyyy-MM-dd HH') < '2020-09-28 15'
    //   AND interface = '/api_v1/user/detail'
    // GROUP BY ip
    // ORDER BY count desc
    // LIMIT 10;

  }

}
