# hive中sql执行顺序

1.from 

2.join on 或 lateral view explode(需炸裂的列) tf as 炸裂后的列名

3.where

4.group by  （开始使用select中的别名，从group 开始往后都可用）

5.聚合函数 如sum() / max() min() / collect_list() / avg()  count(1)等

6.having 对聚合后的结果进行过滤

7.select 中若包含over()开窗函数，执行完非开窗函数后select等待执行完开窗函数，然后执行select完。   
开窗函数通过表数据进行分区和排序，跟select查询中的字段是平行关系，不依赖查询字段。

8.distinct

9.order by 

## 需求1、有一张表两个字段，
uid 用户id，  
day 登陆日期  
求20年9月份 登陆天数超过15的前10个用户。   
```hql
select uid,
       count(*) as cnt
from table_info
where substring(day, 1, 7) = '2020-09'
group by uid
having cnt >= 15
order by cnt desc
limit 10;
```