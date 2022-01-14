# order by 排序
```hql
全局排序，只会有一个reducer
ASC（ascend）: 升序（默认） DESC（descend）: 降序
SELECT * FROM student s 
    LEFT JOIN score sco 
    ON s.s_id = sco.s_id 
ORDER BY sco.s_score DESC;
```
>注意：  
order by 会对输入做全局排序，因此最后只有一个reducer，也就是在一个节点执行，如果数据量太大，就会耗费较长时间。

# sort by 局部排序
```hql
sort by是单个reducer结果有序，不是全局排序。  
因此，如果用sort by进行排序，并且设置mapred.reduce.tasks>1，则sort by只保证每个reducer的输出有序，不保证全局有序。  
-- 设置reduce个数
set mapreduce.job.reduces=3;

-- 查看设置reduce个数
set mapreduce.job.reduces;

-- 查询成绩按照成绩降序排列
select * from score 
    sort by s_score;
 
-- 将查询结果导入到文件中（按照成绩降序排列）
insert overwrite local directory '/export/servers/hivedatas/sort' 
select * from score sort by s_score;
```

# distribute by 分区排序
```hql
distribute by(字段) 根据指定的字段将数据分到不同的reducer，且分发算法是hash散列。
类似MR中partition，进行分区，结合sort by使用

-- 设置reduce的个数，将我们对应的s_id划分到对应的reduce当中去
set mapreduce.job.reduces=7;

-- 通过distribute by 进行数据的分区
select * from score 
    distribute by s_id -- 根据s_id分区，分到不同的reducer
    sort by s_score; --单个reducer内部根据s_score排序
```
>注意：Hive要求 distribute by 语句要写在 sort by 语句之前

# cluster by（= distribute by + sort by）只能正序排序
```hql
当distribute by和sort by字段相同时，可以使用cluster by方式.
cluster by除了具有distribute by的功能外还兼具sort by的功能。
但是排序只能是正序排序，不能指定排序规则为ASC或者DESC。

以下两种写法等价
select * from score cluster by s_id;
select * from score distribute by s_id sort by s_id;
```