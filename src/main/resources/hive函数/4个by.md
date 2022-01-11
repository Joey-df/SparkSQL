# order by 排序
```
全局排序，只会有一个reduce
ASC（ascend）: 升序（默认） DESC（descend）: 降序
SELECT * FROM student s LEFT JOIN score sco ON s.s_id = sco.s_id ORDER BY sco.s_score DESC;
```
>注意：order by 是全局排序，所以最后只有一个reduce，也就是在一个节点执行，如果数据量太大，就会耗费较长时间

# sort by 局部排序
```
单机排序，单个 reduce 结果有序；对全局结果集来说不是有序的。
设置reduce个数
set mapreduce.job.reduces=3;

查看设置reduce个数
set mapreduce.job.reduces;

查询成绩按照成绩降序排列
select * from score sort by s_score;
 
将查询结果导入到文件中（按照成绩降序排列）
insert overwrite local directory '/export/servers/hivedatas/sort' select * from score sort by s_score;
```
# distribute by 分区排序
```
distribute by：类似MR中partition，进行分区，结合sort by使用

设置reduce的个数，将我们对应的s_id划分到对应的reduce当中去
set mapreduce.job.reduces=7;

通过distribute by  进行数据的分区

select * from score distribute by s_id sort by s_score;
```
>注意：Hive要求 distribute by 语句要写在 sort by 语句之前

# cluster by
```
当distribute by和sort by字段相同时，可以使用cluster by方式.
cluster by除了具有distribute by的功能外还兼具sort by的功能。
但是排序只能是正序排序，不能指定排序规则为ASC或者DESC。

以下两种写法等价
select * from score cluster by s_id;
select * from score distribute by s_id sort by s_id;
```