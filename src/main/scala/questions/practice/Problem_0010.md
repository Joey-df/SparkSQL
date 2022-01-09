1.查询"01"课程比"02"课程成绩高的学生的信息及课程分数
```hql
SELECT student.*, 
       a.s_score as s1_score, 
       b.s_score as s2_score
FROM student
JOIN score a ON  a.c_id='01'
JOIN score b ON  b.c_id='02'
WHERE  a.s_id = student.s_id 
  AND b.s_id  =student.s_id AND a.s_score > b.s_score;
```

2.查询"01"课程比"02"课程成绩低的学生的信息及课程分数
```hql
SELECT student.*,
        a.s_score as s1_score,
        b.s_score as s2_score
FROM student
JOIN score a ON  a.c_id='01'
JOIN score b ON  b.c_id='02'
WHERE  a.s_id = student.s_id AND b.s_id = student.s_id AND a.s_score < b.s_score;
```

3.查询平均成绩大于等于60分的同学的学生编号和学生姓名和平均成绩
```hql
SELECT student.s_id,
       student.s_name,
       round(avg(score.s_score), 1) as 平均成绩
FROM student
         JOIN score ON student.s_id = score.s_id
GROUP BY student.s_id, student.s_name
HAVING avg(score.s_score) >= 60;
```

4.查询平均成绩小于60分的同学的学生编号和学生姓名和平均成绩
```hql
-- 包括有成绩的和无成绩的
SELECT student.s_id,
       student.s_name,
       tmp.avgScore
FROM student
         JOIN
     (
         select score.s_id,
                round(avg(score.s_score), 1) as avgScore
         FROM score
         group by s_id
     ) tmp ON tmp.avgScore < 60
WHERE student.s_id = tmp.s_id

UNION ALL

SELECT s2.s_id,
       s2.s_name,
       0 as avgScore
FROM student s2
WHERE s2.s_id NOT IN (select distinct sc2.s_id FROM score sc2);
```

5.查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
```hql
SELECT student.s_id,
       student.s_name,
       count(score.c_id)  as total_count, --选课总数
       sum(score.s_score) as total_score -- 所有课程的总成绩
FROM student
         LEFT JOIN score ON student.s_id = score.s_id
GROUP BY student.s_id,
         student.s_name;
```

6.查询"李"姓老师的数量
```hql
SELECT count(1) cnt --"李"姓老师的数量
FROM teacher
WHERE t_name LIKE '李%';
```

7.查询学过"张三"老师授课的同学的信息
```hql
SELECT student.*
FROM student
         JOIN score ON student.s_id = score.s_id
         JOIN course ON course.c_id = score.c_id
         JOIN teacher ON course.t_id = teacher.t_id AND t_name = '张三';
```

8.查询没学过"张三"老师授课的同学的信息
```hql
SELECT student.*
FROM student
         LEFT JOIN
     (select s_id
      FROM score
               JOIN course ON course.c_id = score.c_id
               JOIN teacher ON course.t_id = teacher.t_id and t_name = '张三') tmp -- 上过张三老师课的学生ID
     ON student.s_id = tmp.s_id
WHERE tmp.s_id is null;
```

9.查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
```hql
SELECT *
from student
         JOIN (select s_id FROM score WHERE c_id = '01') t1
              ON student.s_id = t1.s_id
         JOIN (select s_id FROM score WHERE c_id = '02') t2
              ON student.s_id = t2.s_id;
```

10.查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息
```hql
SELECT student.*
FROM student
         JOIN(select s_id FROM score WHERE c_id = '01') tmp1 on student.s_id = tmp1.s_id
         LEFT JOIN (select s_id FROM score WHERE c_id = '02') tmp2 on student.s_id = tmp2.s_id
WHERE tmp2.s_id is null;
```

11.查询没有学全所有课程的同学的信息
```hql
select student.*
from student
         join (select count(c_id) num1 from course) tmp1 -- 课程总数信息
         left join(select s_id, count(c_id) num2 from score group by s_id) tmp2 -- 每个学生学完的课程数量信息
                  on student.s_id = tmp2.s_id and tmp1.num1 = tmp2.num2
where tmp2.s_id is null;
```

12. 查询至少有一门课与学号为"01"的同学所学相同的同学的信息
```hql
select student.*
from student
         join (select c_id from score where score.s_id = '01') tmp1 --学号为"01"的同学所学课程c_id信息
         join (select s_id, c_id from score) tmp2 -- 学生与课程信息（来自成绩表）
              on tmp1.c_id = tmp2.c_id and student.s_id = tmp2.s_id
where student.s_id not in ('01')
group by student.s_id, s_name, s_birth, s_sex;
```

13. 查询和"01"号的同学学习的课程完全相同的其他同学的信息
```hql
select student.*, tmp1.course_id
from student
         join
     (
         select s_id,
                concat_ws('|', collect_set(c_id)) course_id
         from score
         where s_id not in ('01')
         group by s_id
     ) tmp1 -- "01"号之外的同学的选课信息
     on student.s_id = tmp1.s_id
         join
     (
         select concat_ws('|', collect_set(c_id)) course_id_set from score where s_id = '01'
     ) tmp2 -- "01"号同学学习的课程
     on tmp1.course_id = tmp2.course_id_set;
```

14.查询没学过"张三"老师讲授的任一门课程的学生姓名
```hql
select student.*
from student
         left join
     (
         select s_id
         from score
                  join
              (
                  select c_id
                  from course
                           join teacher on course.t_id = teacher.t_id and t_name = '张三'
              ) tmp2 -- "张三"老师讲授的课程信息c_id
              on score.c_id = tmp2.c_id
     ) tmp -- 听过"张三"老师讲过课的学生s_id
     on student.s_id = tmp.s_id
where tmp.s_id is null; -- 左联tmp，tmp.s_id is null就表示没有听过"张三"讲过任一门课的学生信息
```

15.查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
```hql
select s.s_id, s.s_name, tmp.avg_score
from student s
         inner join
     (
         select s_id
         from score
         where s_score < 60
         group by s_id
         having count(s_id) >= 2
     ) tmp2 -- 两门及其以上不及格课程的学生信息
     on s.s_id = tmp2.s_id
         left join
     (
         select s_id,
                round(avg(s_score)) avg_score
         from score
         group by s_id
     ) tmp
     on tmp.s_id = s.s_id;
```

16.检索"01"课程分数小于60，按分数降序排列的学生信息
```hql
-- 方法1
select student.*, tmp.s_core
from student s
         join
     (
         select s_id, s_core
         from score
         where c_id = '01'
           and s_score < 60
     ) tmp -- "01"课程分数小于60分的学生编号、对应分数
     on s.s_id = tmp.s_id
order by tmp.s_core desc;

--方法2
select student.*, s_score
from student,
     score
where student.s_id = score.s_id
  and s_score < 60
  and c_id = '01'
order by s_score desc;
```

17.按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
```hql
-- 行转列的场景
select a.s_id,
       tmp1.s_score             as chinese, --语文
       tmp2.s_score             as math, --数学
       tmp3.s_score             as english, --英语
       round(avg(a.s_score), 2) as avgScore
from score a
         left join (select s_id, s_score from score s1 where c_id = '01') tmp1 on tmp1.s_id = a.s_id
         left join (select s_id, s_score from score s2 where c_id = '02') tmp2 on tmp2.s_id = a.s_id
         left join (select s_id, s_score from score s3 where c_id = '03') tmp3 on tmp3.s_id = a.s_id
group by a.s_id, tmp1.s_score, tmp2.s_score, tmp3.s_score
order by avgScore desc;
```

18.查询各科成绩最最高分，最低分，平均分，及格率，中等率，优良率，优秀率
```hql
-- 以如下形式显示：课程ID，课程name，最高分，最低分，平均分，及格率，中等率，优良率，优秀率
-- 及格为>=60，中等为：70-80，优良为：80-90，优秀为：>=90
select course.c_id,       --课程ID
       course.c_name,     --课程name
       tmp.maxScore,      --最高分
       tmp.minScore,      --最低分
       tmp.avgScore,      --平均分
       tmp.passRate,      --及格率
       tmp.modeRate,      --中等率
       tmp.goodRate,      --优良率
       tmp.excellentRates --优秀率
from course
         join
     (
         select c_id,
                max(s_score) as                                                                         maxScore,--最高分
                min(s_score) as                                                                         minScore,--最低分
                round(avg(s_score), 2)                                                                  avgScore,--平均分
                round(sum(case when s_score >= 60 then 1 else 0 end) / count(c_id), 2)                  passRate,--及格率
                round(sum(case when s_score >= 70 and s_score < 80 then 1 else 0 end) / count(c_id), 2) modeRate,--中等率
                round(sum(case when s_score >= 80 and s_score < 90 then 1 else 0 end) / count(c_id), 2) goodRate,--优良率
                round(sum(case when s_score >= 90 then 1 else 0 end) / count(c_id), 2)                  excellentRates --优秀率
         from score
         group by c_id
     ) tmp
     on tmp.c_id = course.c_id;
```

19. 按各科成绩进行排序，并显示排名
```hql
select s1.*, row_number() over (order by s1.s_score desc) rank
from score s1
where s1.c_id = '01'
order by rank asc

union all

select s2.*, row_number() over (order by s2.s_score desc) rank
from score s2
where s2.c_id = '02'
order by rank asc

union all

select s3.*, row_number() over (order by s3.s_score desc) rank
from score s3
where s3.c_id = '03'
order by rank asc;
```

20.查询学生的总成绩并进行排名
```hql
--方法1
select score.s_id,
       student.s_name,
       sum(s_score)                             sum_score,
       rank() over (order by sum(s_score) desc) rank --整张表作为一个窗口进行排名(按总分排名)
from score,
     student
where score.s_id = student.s_id
group by score.s_id, student.s_name
order by sum_score desc;

--方法2
select student.s_id,
       student.s_name,
       sum_score,
       rank() over (order by sum_score desc ) rank
from (
         select student.s_id,
                student.s_name,
                sum(score.s_score) sum_score
         from score
                  join student
                       on score.s_id = student.s_id
         group by student.s_id,
                  student.s_name
         order by sum_score desc
     ) tmp;
```

21.查询不同老师所教不同课程平均分从高到低显示
```hql
select course.c_id,
       course.t_id,
       teacher.t_name,
       round(avg(score.s_score), 2) as avgscore
from course
         join teacher on teacher.t_id = course.t_id
         join score on course.c_id = score.c_id
group by course.c_id, course.t_id, teacher.t_name
order by avgscore desc;
```

22.查询所有课程的成绩第2名到第3名的学生信息及该课程成绩
```hql
select tmp1.*
from (select * from score where c_id = '01' order by s_score desc limit 3) tmp1 --最高的三个（1/2/3名）
order by s_score asc limit 2 -- 升序排取2个，刚好就是 第2名 和 第3名

union all

select tmp2.*
from (select * from score where c_id = '02' order by s_score desc limit 3) tmp2
order by s_score asc limit 2

union all

select tmp3.*
from (select * from score where c_id = '03' order by s_score desc limit 3) tmp3
order by s_score asc limit 2;
```

23.统计各科成绩各分数段人数
输出：课程编号,课程名称,[100-85],[85-70],[70-60],[0-60]及所占百分比
```hql
--方法1
select c.c_id,
       c.c_name,
       tmp1.s0_60,     -- [0-60]人数
       tmp1.percentum, -- [0-60]人数所占百分比
       tmp2.s60_70,
       tmp2.percentum,
       tmp3.s70_85,
       tmp3.percentum,
       tmp4.s85_100,
       tmp4.percentum
from course c
         join(select c_id,
                     sum(case when s_score < 60 then 1 else 0 end)                               as s0_60,
                     round(100 * sum(case when s_score < 60 then 1 else 0 end) / count(c_id), 2) as percentum
              from score
              group by c_id) tmp1 on tmp1.c_id = c.c_id
         left join(select c_id,
                          sum(case when s_score < 70 and s_score >= 60 then 1 else 0 end) as s60_70,
                          round(100 * sum(case when s_score < 70 and s_score >= 60 then 1 else 0 end) / count(c_id),
                                2)                                                        as percentum
                   from score
                   group by c_id) tmp2 on tmp2.c_id = c.c_id
         left join(select c_id,
                          sum(case when s_score < 85 and s_score >= 70 then 1 else 0 end) as s70_85,
                          round(100 * sum(case when s_score < 85 and s_score >= 70 then 1 else 0 end) / count(c_id),
                                2)                                                        as percentum
                   from score
                   group by c_id) tmp3 on tmp3.c_id = c.c_id
         left join(select c_id,
                          sum(case when s_score >= 85 then 1 else 0 end)                               as s85_100,
                          round(100 * sum(case when s_score >= 85 then 1 else 0 end) / count(c_id), 2) as percentum
                   from score
                   group by c_id) tmp4 on tmp4.c_id = c.c_id;

--方法2
select c.c_id,
       c.c_name,
       tmp.s0_60,                                              -- [0-60]人数
       round(100 * (tmp.s0_60 / c_total), 2)   percentum_0_60, -- [0-60]人数所占百分比
       tmp.s60_70,
       round(100 * (tmp.s60_70 / c_total), 2)  percentum_60_70,
       tmp.s70_85,
       round(100 * (tmp.s70_85 / c_total), 2)  percentum_70_85,
       tmp.s85_100,
       round(100 * (tmp.s85_100 / c_total), 2) percentum_85_100
from course c
         join
     (
         select c_id,
                count(c_id)                                                        c_total,
                sum(case when s_score < 60 then 1 else 0 end)                   as s0_60,
                sum(case when s_score < 70 and s_score >= 60 then 1 else 0 end) as s60_70,
                sum(case when s_score < 85 and s_score >= 70 then 1 else 0 end) as s70_85,
                sum(case when s_score >= 85 then 1 else 0 end)                  as s85_100
         from score
         group by c_id
     ) tmp
     on c.c_id = tmp.c_id;
```

24.查询学生平均成绩及其名次
```hql
select tmp.*,
       rank() over (order by tmp.avgScore desc) rank --整张表开窗，按平均分整体排名
from (
         select student.s_id,
                student.s_name,
                round(avg(score.s_score), 2) as avgScore
         from student
                  join score
                       on student.s_id = score.s_id
         group by student.s_id, student.s_name
     ) tmp
order by avgScore desc;
```

25.查询各科成绩前三名的记录
```hql
-- 1.课程id为01的成绩前三名
select score.c_id, course.c_name, student.s_name, s_score
from score
         join student on student.s_id = score.s_id
         join course on score.c_id = '01' and course.c_id = score.c_id
order by s_score desc
limit 3;

-- 2.课程id为02的成绩前三名
select score.c_id, course.c_name, student.s_name, s_score
from score
         join student on student.s_id = score.s_id
         join course on score.c_id = '02' and course.c_id = score.c_id
order by s_score desc
limit 3;

-- 3.课程id为03的成绩前三名
select score.c_id, course.c_name, student.s_name, s_score
from score
         join student on student.s_id = score.s_id
         join course on score.c_id = '03' and course.c_id = score.c_id
order by s_score desc
limit 3;
```

26.查询每门课程被选修的学生数
```hql
select c_id,
       count(s_id) stu_num
from (
         select c_id, s_id
         from score
         group by c_id, s_id
     ) tmp
group by c_id;
```

27.查询出只有两门课程的全部学生的学号和姓名
```hql
select st.s_id, st.s_name
from student st
         join
     (
         select s_id
         from score
         group by s_id
         having count(c_id) = 2
     ) tmp -- 只有两门课程的学生s_id
     on st.s_id = tmp.s_id;
```

28.查询男生、女生人数
```hql
-- 方法1
select tmp1.man, tmp2.women
from (select count(1) as man from student where s_sex = '男') tmp1,
     (select count(1) as women from student where s_sex = '女') tmp2;
--方法2
select sum(case when s_sex = '男' then 1 else 0 end) man_num,
       sum(if(s_sex = '女', 1, 0))                   women_num
from student;
```

29.查询名字中含有"风"字的学生信息
```hql
select * from student where s_name like '%风%';
```

30.查询同名同性学生名单，并统计同名人数
```hql
-- 待测试
--方法1
select s1.s_name,
       s1.s_sex,
       count(*) as same_cnt --同名同姓的人数
from student s1,
     student s2
where s1.s_name = s2.s_name --同名
  and s1.s_sex = s2.s_sex   --同性
  and s1.s_id <> s2.s_id
group by s1.s_name, s1.s_sex;

--方法2
select s1.s_name,
       s1.s_sex,
       count(*) as same_cnt --同名同姓的人数
from student s1
         join
     student s2
     on s1.s_name = s2.s_name --同名
         and s1.s_sex = s2.s_sex --同性
         and s1.s_id <> s2.s_id
group by s1.s_name, s1.s_sex;
```

31.查询1990年出生的学生名单
```hql
--方法1
select * from student where s_birth like '1990%';
--方法2
select * from student where year(s_birth) = '1990';
--方法3
select * from student where substring(s_birth,1,4) = '1990';
```

32.查询每门课程的平均成绩，结果按平均成绩降序排列，平均成绩相同时，按课程编号升序排列
```hql
select score.c_id,
       course.c_name,
       round(avg(score.s_score), 2) as avgScore
from score
         join course on score.c_id = course.c_id
group by score.c_id, course.c_name
order by avgScore desc, score.c_id asc;
```

33.查询平均成绩大于等于85的所有学生的学号、姓名和平均成绩
```hql
select score.s_id,
       s.s_name,
       round(avg(s_score), 2) as avgScore
from score
         join student s on s.s_id = score.s_id
group by score.s_id, s.s_name
having avg(s_score) >= 85;
```

34.查询课程名称为"数学"，且分数低于60的学生姓名和分数
```hql
--方法1
select s_name,
       s_score as mathScore
from student
         join
     (
         select s.s_id,
                s.s_score
         from score s,
              course c
         where s.c_id = c.c_id
           and c.c_name = '数学'
     ) tmp
     on tmp.s_score < 60 and student.s_id = tmp.s_id;

--方法2
select s_name,
       s_score as mathScore
from student
         join
     (
         select s.s_id,
                s.s_score
         from score s
                  join
                  (select * from course c where c.c_name = '数学') tt
                  on s.c_id = tt.c_id
     ) tmp
     on tmp.s_score < 60 and student.s_id = tmp.s_id;
```

35.查询所有学生的 语数外各门课程分数、以及总分情况
```hql
-- 有行转列的意思
select a.s_id,
       a.s_name,
       SUM(case c.c_name when '语文' then b.s_score else 0 end) as chinese,
       SUM(case c.c_name when '数学' then b.s_score else 0 end) as math,
       SUM(case c.c_name when '英语' then b.s_score else 0 end) as english,
       SUM(b.s_score)                                         as sumScore
from student a
         join score b on a.s_id = b.s_id
         join course c on b.c_id = c.c_id
group by a.s_id, s_name;
```

36.查询任何一门课程成绩在70分以上的学生姓名、课程名称和分数
```hql
-- 查询任何一门课程成绩在70分以上的学生姓名、课程名称和分数（重点复习）
select s.s_id,
       s.s_name,
       c.c_name,
       tmp2.s_score
from student s
         join
     (
         select sc.*
         from score sc
                  left join
              (
                  select s_id
                  from score
                  where s_score < 70
                  group by s_id
              ) tmp --存在成绩在70分以下的学生id（去重后的）
              on sc.s_id = tmp.s_id
         where tmp.s_id is null --得到任何一门课程成绩在70分以上的学生（重点复习）
     ) tmp2
     on s.s_id = tmp2.s_id
         join course c on tmp2.c_id = c.c_id
order by s_id;

-- 查询全部及格的信息
-- 方法1（重点复习）
select sc.*
from score sc
         left join
     (
         select s_id
         from score
         where s_score < 60
         group by s_id
     ) tmp -- 不及格的学生id（去重后的）
     on sc.s_id = tmp.s_id
where tmp.s_id is null;

-- 方法2(效率低)
select sc.*
from score sc
where sc.s_id not in (select s_id from score where s_score < 60 group by s_id);
```

37.查询课程不及格的学生
```hql
--方法1
select s_name,     --学生姓名
       c_name,     -- 课程名称
       tmp.s_score -- 分数
from student
         join
     (
         select s_id,
                s_score,
                c_name
         from score,
              course
         where score.c_id = course.c_id
           and s_score < 60
     ) tmp
     on student.s_id = tmp.s_id;

--方法2
select stu.s_name, --学生姓名
       tmp.c_name, -- 课程名称
       tmp.s_score -- 分数
from student stu
         join
     (
         select s.s_id,
                s.s_score,
                c.c_name
         from course c
                  join
              (
                  select *
                  from score
                  where s_score < 60
              ) s
              on s.c_id = c.c_id
     ) tmp
     on stu.s_id = tmp.s_id;
```

38.查询课程编号为01且课程成绩在80分以上的学生的学号和姓名
```hql
select s.s_id,
       s.s_name,
       s_score as score_01
from student s
         join score on s.s_id = score.s_id
where score.c_id = '01'
  and s_score >= 80;
```

39.求每门课程的选课学生人数
```hql
select c.c_id,
       c.c_name,
       count(1) as selectNum
from course c
         join score on c.c_id = score.c_id
group by c.c_id, c.c_name;
```

40.查询选修"张三"老师所授课程的学生中，成绩最高的学生信息及其成绩
```hql
select student.*,
       tmp3.c_name,
       tmp3.maxScore
from (
         select s_id,
                c_name,
                max(s_score) as maxScore
         from score
                  join
              (
                  select course.c_id, c_name
                  from course
                           join
                           (select t_id, t_name from teacher where t_name = '张三') tmp
                           on course.t_id = tmp.t_id
              ) tmp2 -- "张三"老师所授课程信息
              on score.c_id = tmp2.c_id
         group by score.s_id, c_name
         order by maxScore desc
         limit 1 -- 得到选修"张三"老师所授课程的学生中，成绩最高的学生s_id、课程名称、成绩等信息
     ) tmp3
         join student
              on student.s_id = tmp3.s_id;
```

41.查询不同课程成绩相同的学生的学生编号、课程编号、学生成绩
```hql
select distinct a.s_id,
                a.c_id,
                a.s_score
from score a,
     score b
where a.c_id <> b.c_id --不同课程
  and a.s_score = b.s_score; --成绩相同
```

42.查询每门课程成绩最好的前三名
```hql
--方法1
select tmp1.*
from (select *, row_number() over (order by s_score desc) ranking
      from score
      where c_id = '01') tmp1
where tmp1.ranking <= 3

union all

select tmp2.*
from (select *, row_number() over (order by s_score desc) ranking
      from score
      where c_id = '02') tmp2
where tmp2.ranking <= 3

union all

select tmp3.*
from (select *, row_number() over (order by s_score desc) ranking
      from score
      where c_id = '03') tmp3
where tmp3.ranking <= 3;


--方法2
select s_id,
       c_id,
       s_score,
       rk
from (
         select score.s_id,
                score.c_id,
                score.s_score,
                row_number() over (partition by c_id order by s_score desc) rk
         from score
     ) tmp
where rk <= 3
order by s_id, rk;
```

43.统计每门课程的学生选修人数（超过5人的课程才统计）
```hql
-- 统计每门课程的学生选修人数（超过5人的课程才统计）
-- 要求输出课程号和选修人数，查询结果按人数降序排列，若人数相同，按课程号升序排列
select distinct course.c_id,
                tmp.num
from course
         join
     (
         select c_id,
                count(1) as num
         from score
         group by c_id
         having count(1) >= 5
     ) tmp
order by tmp.num desc, course.c_id asc; -- 按人数降序排列，若人数相同，按课程号升序排列
```

44.检索至少选修两门课程的学生学号
```hql
select s_id,
       count(c_id) as totalCourse
from score
group by s_id
having count(c_id) >= 2;
```

45.查询选修了全部课程的学生信息
```hql
--方法1
select student.*
from student,
     (
         select s_id,
                count(c_id) as totalCourse
         from score
         group by s_id
     ) tmp
where student.s_id = tmp.s_id
  and totalCourse = 3;

--方法2
select s.*
from student s
         join
     (
         select s_id,
                count(c_id)
         from score
         group by s_id
         having count(c_id) = 3
     ) tmp
     on s.s_id = tmp.s_id;

--方法3
select s.*
from student s
where s.s_id in (
    select s_id,
           count(c_id) selectedNum
    from score
    group by s_id
    having count(c_id) == 3)
````

46.查询10月份过生日的学生
```hql
select s_name, s_sex, s_birth
from student
where substring(s_birth, 6, 2) = '10';

--方法2
select s_name, s_sex, s_birth
from student
where month(s_birth) = '10';
```

47.查询本月过生日的学生
```hql
select s_name, s_sex, s_birth
from student
where month(s_birth) = month(CURRENT_DATE);
```

48.查询各学生的年龄（周岁）
```hql
-- 查询各学生的年龄（周岁）
-- 按照出生日期来算，当前月日 < 出生年月的月日则，年龄减一
select s_name,
       s_birth,
       (year(CURRENT_DATE) - year(s_birth) -
        (case
             when month(CURRENT_DATE) < month(s_birth) then 1
             when month(CURRENT_DATE) = month(s_birth) and day(CURRENT_DATE) < day(s_birth) then 1
             else 0 end)
           ) as age
from student;
```