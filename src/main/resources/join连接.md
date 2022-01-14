# join 连接

```hql
INNER JOIN 内连接：只有进行连接的两个表中都存在与连接条件相匹配的数据才会被保留下来
select * from techer t join course c on t.t_id = c.t_id; -- inner 可省略

LEFT OUTER JOIN 左外连接：左边所有数据会被返回，右边符合条件的被返回
select * from techer t left join course c on t.t_id = c.t_id; -- outer可省略

RIGHT OUTER JOIN 右外连接：右边所有数据会被返回，左边符合条件的被返回、
select * from techer t right join course c on t.t_id = c.t_id;

FULL OUTER JOIN 满外(全外)连接: 将会返回所有表中符合条件的所有记录。如果任一表的指定字段没有符合条件的值的话，那么就使用NULL值替代。
SELECT * FROM techer t FULL JOIN course c ON t.t_id = c.t_id ;
```
>注：
>1.hive2版本已经支持不等值连接，就是 join on条件后面可以使用大于小于符号了;并且也支持 join on 条件后跟or (早前版本 on 后只支持 = 和 and，不支持 > < 和 or)    
 2.如hive执行引擎使用MapReduce，一个join就会启动一个job，一条sql语句中如有多个join，则会启动多个job。    
 
>注意：表之间用逗号(,)连接和 inner join 是一样的   
 select * from table_a,table_b where table_a.id=table_b.id;   
 它们的执行效率没有区别，只是书写方式不同，用逗号是sql 89标准，join 是sql 92标准。用逗号连接后面过滤条件用 where ，用 join 连接后面过滤条件是 on。   
