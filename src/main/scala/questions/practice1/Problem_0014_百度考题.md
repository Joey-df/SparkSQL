## 百度考题
现在有三个表student（学生表）、course(课程表)、score（成绩单），结构如下：
```hql
create table student
(
 id bigint comment ‘学号’，
 name string comment ‘姓名’,
 age bigint comment ‘年龄’
);

create table course
(
 cid string comment ‘课程号，001/002格式’,
 cname string comment ‘课程名’
);

Create table score
(
 id bigint comment ‘学号’,
 cid string comment ‘课程号’,
 score bigint comment ‘成绩’
) partitioned by(event_day string)
```
其中score中的id、cid，分别是student、course中对应的列。

请根据上面的表结构，回答下面的问题：

1）请将本地文件（/home/users/test/20190301.csv）文件，加载到分区表score的20190301分区中，并覆盖之前的数据

2）查出平均成绩大于60分的学生的姓名、年龄、平均成绩

3）查出没有‘001’课程成绩的学生的姓名、年龄

4）查出有‘001’\’002’这两门课程下，成绩排名前3的学生的姓名、年龄

5）创建新的表score_20190317，并存入score表中20190317分区的数据

6）如果上面的score表中，uid存在数据倾斜，请进行优化，查出在20190101-20190317中，学生的姓名、年龄、课程、课程的平均成绩

7）描述一下union和union all的区别，以及在mysql和HQL中用法的不同之处？

8）简单描述一下lateral view语法在HQL中的应用场景，并写一个HQL实例。

