package questions.practice

/**
 * 第十题
 * 存在如下表：
 * table student(s_id string, s_name string, s_birth string, s_sex string)
 * table course(c_id string, c_name string, t_id string)
 * table teacher(t_id string, t_name string)
 * table score(s_id string, c_id string, s_score int)
 *
 * 示例数据：
 *
 * student：
 * s_id  s_name  s_birth  s_sex
 * 01 赵雷 1993-01-01 男
 * 02 钱电 1989-12-21 男
 * 03 孙雷 2000-05-20 男
 * 04 李云 1990-08-06 男
 * 05 周天 1978-12-01 女
 * 06 吴兰 1992-03-01 女
 * 07 郑竹 1989-07-01 男
 * 08 王霞 1993-01-20 女
 *
 * course：
 * c_id c_name  t_id
 * 01   语文      02
 * 02   数学      01
 * 03   英语      03
 *
 * teacher：
 * t_id t_name
 * 01  张三
 * 02  李四
 * 03  王五
 *
 * score：
 * s_id  c_id  s_score
 * 01     01      80
 * 01     02      90
 * 01     03      99
 * 02     01      70
 * 02     02      60
 * 02     03      80
 * 03     01      80
 * 03     02      80
 * 03     03      80
 *
 * 分别实现以下需求：
 *
 * 1.查询"01"课程比"02"课程成绩高的学生的信息及课程分数
 * 2.查询"01"课程比"02"课程成绩低的学生的信息及课程分数
 * 3.查询平均成绩大于等于60分的同学的学生编号和学生姓名和平均成绩
 * 4.查询平均成绩小于60分的同学的学生编号和学生姓名和平均成绩
 * 5.查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
 * 6.查询"李"姓老师的数量
 * 7.查询学过"张三"老师授课的同学的信息
 * 8.查询没学过"张三"老师授课的同学的信息
 * 9.查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
 * 10.查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息
 * 11.查询没有学全所有课程的同学的信息
 * 12. 查询至少有一门课与学号为"01"的同学所学相同的同学的信息
 * 13. 查询和"01"号的同学学习的课程完全相同的其他同学的信息
 * 14.查询没学过"张三"老师讲授的任一门课程的学生姓名
 * 15.查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
 * 16.检索"01"课程分数小于60，按分数降序排列的学生信息
 * 17.按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
 * 18.查询各科成绩最高分、最低分和平均分
 * 19. 按各科成绩进行排序，并显示排名
 * 20.查询学生的总成绩并进行排名
 * 21.查询不同老师所教不同课程平均分从高到低显示
 * 22.查询所有课程的成绩第2名到第3名的学生信息及该课程成绩
 * 23.统计各科成绩各分数段人数
 * 24.查询学生平均成绩及其名次
 * 25.查询各科成绩前三名的记录
 * 26.查询每门课程被选修的学生数
 * 27.查询出只有两门课程的全部学生的学号和姓名
 * 28.查询男生、女生人数
 * 29.查询名字中含有"风"字的学生信息
 * 30.查询同名同性学生名单，并统计同名人数
 * 31.查询1990年出生的学生名单
 * 32.查询每门课程的平均成绩，结果按平均成绩降序排列，平均成绩相同时，按课程编号升序排列
 * 33.查询平均成绩大于等于85的所有学生的学号、姓名和平均成绩
 * 34.查询课程名称为"数学"，且分数低于60的学生姓名和分数
 * 35.查询所有学生的课程及分数情况
 * 36.查询任何一门课程成绩在70分以上的学生姓名、课程名称和分数
 * 37.查询课程不及格的学生
 * 38.查询课程编号为01且课程成绩在80分以上的学生的学号和姓名
 * 39.求每门课程的学生人数
 * 40.查询选修"张三"老师所授课程的学生中，成绩最高的学生信息及其成绩
 * 41.查询不同课程成绩相同的学生的学生编号、课程编号、学生成绩
 * 42.查询每门课程成绩最好的前三名
 * 43.统计每门课程的学生选修人数（超过5人的课程才统计）
 * 44.检索至少选修两门课程的学生学号
 * 45.查询选修了全部课程的学生信息
 * 46.查询下周过生日的学生
 * 47.查询本月过生日的学生
 * 48.查询12月份过生日的学生
 * 49.查询各学生的年龄（周岁）
 * 50.查询本周过生日的学生
 *
 */
object Problem_0010 {

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
