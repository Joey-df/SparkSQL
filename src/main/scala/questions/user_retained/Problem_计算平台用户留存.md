## 题目描述
https://shimo.im/docs/jwkxXhVQJ3Q9rKrk/read
https://blog.csdn.net/Aeve_imp/article/details/105878068
https://blog.csdn.net/u013411339/article/details/120232456

用户留存是体现平台健康程度的重要数据指标之一，请用hive sql完成如下需求：   
已知：   
#### 用户信息表结构：     
```hql
CREATE TABLE if NOT EXISTS dim_tb_user
(
    user_id     BIGINT COMMENT "用户id",
    create_time STRING COMMENT "注册时间,如：2019-01-01 08:00:00"
) COMMENT "用户信息表";
```
#### 埋点访问日志表结构:
```hql
CREATE TABLE if NOT EXISTS fact_access_log
(
    user_id     BIGINT COMMENT "用户id",
    page_url    STRING COMMENT "网站页面路径,用于标记不同页面",
    create_time STRING COMMENT "访问时间,如：2019-01-01 08:00:01"
) COMMENT "用户访问日志表" PARTITIONED by (dt STRING COMMENT "格式：yyyymmdd ,数值与create_time转化后的日期一致";
```
dim_tb_user: 总量数百万行级别   
| user_id | create_time  |    
| --- | --- |    
| 123456 | 2018-09-01 08:00:01   |    
| 123457 | 2019-05-24 09:21:42 |    
| 123458 | 2019-08-04 21:02:12 |   
| ... | ... |    

fact_access_log ：每天新增访问日志记录数据量数亿行   
| user_id | page_url  | create_time  | dt  |     
| --- | --- | --- | --- |     
| 123456 | index | 2018-09-01 08:08:32 |  20180901  |     
| 123457 | hot | 2019-05-24 09:21:46 |  20190524  |     
| 123458 | mine | 2019-08-04 22:01:21 |  20190804  |     
| ... | ... | ... | ...  |     

>定义：   
新用户：某一日新注册的用户     
新用户次1日留存：T日成为新用户，T+1日回访平台网站任意页面（T+1即T日期的第二天）       
新用户次3日留存：T日成为新用户，T+3日回访平台网站任意页面      
新用户次n日留存：T日成为新用户，T+n日回访平台网站任意页面    

1.假设平台2019年元旦节做了一次拉新活动，当日新增用户数约为平日的两倍，2019年1月9日，距离活动已经过去一周了，   
公司希望评估一下2019年1月1日当天的全部新增用户，在接下来一周的留存情况(即1月2日-1月8日每天的留存用户数)，    
要求交付的数据结果表如下，请用一个简洁的sql计算出如下数据：   

所获得的结果如下：  
| 新用户数 | 次1日留存用户数  | 次2日留存用户数  |  次3日留存用户数 | 次4日留存用户数  | 次5日留存用户数  |  次6日留存用户数 | 次7日留存用户数  |  dt（成为新用户日期） |     
| --- | --- | --- | --- | --- | --- | --- | --- | --- |     
| 61132 | 5054 | 4284 | 3492 | 3209 | 3202 | 2929 | 2116 | 20190101 |     
```hql
select count(distinct user.user_id)                   as new_users,
       count(case when tmp.gap = 1 then 1 else 0 end) as save_1,
       count(case when tmp.gap = 2 then 1 else 0 end) as save_2,
       count(case when tmp.gap = 3 then 1 else 0 end) as save_3,
       count(case when tmp.gap = 4 then 1 else 0 end) as save_4,
       count(case when tmp.gap = 5 then 1 else 0 end) as save_5,
       count(case when tmp.gap = 6 then 1 else 0 end) as save_6,
       count(case when tmp.gap = 7 then 1 else 0 end) as save_7,
       '20190101'                                     as dt
from (
         select user.user_id,
                (case when log.dt is null then -1 else datediff(date_format(log.dt, 'yyyy-MM-dd'), '2019-09-01') end) as gap
         from (
                  --'2019-09-01'的新用户
                  select *
                  from dim_tb_user
                  where date_format(create_time, 'yyyy-MM-dd') = '2019-09-01'
              ) user
              left join
              (
                --'2019-09-02'及以后每天的用户访问日志
                select * from fact_access_log
                where dt > 20190901 and date_format(create_time, 'yyyy-MM-dd') > '2019-09-01'
              ) log
         on user.user_id = log.user_id
         group by user.user_id,
             datediff(date_format(log.dt, 'yyyy-MM-dd'), '2019-09-01')
     ) tmp
group by '20190101';
```


2.假设本次活动拉新留存情况符合预期，公司决定后续会不定期举办拉新活动，    
以获得更多新用户，结合你所掌握的数仓知识，设计调度任务，使得报表按天增量更新，帮助公司长期监控平台每一天的新用户留存。   
（请给出完整的相关sql，将数据写入结果表中）

补充：
1. 希望保留全量历史数据，方便随时对比各期活动效果，即：将历史产出的数据永久保存在当日分区中，随时可查询历史任意日期的计算结果。
2. 日期未到，留存数暂时用0补充，如：2019年1月9日那天，计算1月8日的次7日留存数，此时因为次7日时间未到而计算不出真实值，将其设置为0即可，待到了可以计算的对应日期再将数据更新。
3. 请考虑大数据的特点，合理设计sql计算方案，尽可能的降低数据计算量，避免产出时间过长，资源消耗过大。   
4. 保持数据幂等性，支持任意时间段历史数据回溯：若遇到异常情况可重跑N天前的数据将其快速修复。  
5. 合理设计增量更新方案，切勿做全量计算。  
6. 保持sql尽可能精简易读。  
（以下数据范例最后更新日期为：20190109）   
输出结果表结构：
```hql
CREATE TABLE if NOT EXISTS reporting_new_user_retain_day
(
    new_cnt BIGINT COMMENT "新增用户数",
    retain1 BIGINT COMMENT "次1日留存用户数",
    retain2 BIGINT COMMENT "次2日留存用户数",
    retain3 BIGINT COMMENT "次3日留存用户数",
    retain4 BIGINT COMMENT "次4日留存用户数",
    retain5 BIGINT COMMENT "次5日留存用户数",
    retain6 BIGINT COMMENT "次6日留存用户数",
    retain7 BIGINT COMMENT "次7日留存用户数"
) COMMENT "新增用户留存报表" PARTITIONED by (dt STRING COMMENT "格式：yyyymmdd ,成为新用户的日期";
```
| new_cnt | retain1  | retain2  |  retain3 | retain4  | retain5  |  retain6 | retain7  | dt |     
| --- | --- | --- | --- | --- | --- | --- | --- | --- |     
| 33492 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 20190108 |    
| 33679 | 3452 | 0 | 0 | 0 | 0 | 0 | 0 | 20190107 |    
| 34657 | 3253 | 2662 | 0 | 0 | 0 | 0 | 0 | 20190106 |    
| 35264 | 3582 | 2865 | 1627 | 0 | 0 | 0 | 0 | 20190105 |    
| 33266 | 3281 | 2660 | 1619 | 1397 | 0 | 0 | 0 | 20190104 |    
| 37125 | 3620 | 2761 | 1897 | 1697 | 1372 | 0 | 0 | 20190103 |    
| 31206 | 3121 | 2465 | 1597 | 1078 | 995 | 991 | 0 | 20190102 |    
| 61132 | 3054 | 2284 | 1398 | 1009 | 872 | 768 | 673 | 20190101 |    
| 30206 | 3021 | 2165 | 1397 | 971 | 873 | 567 | 499 | 20181231 |    
| ... | ... | ... | ... | ... | ... | ... | ... | ... |   
