## 题目描述
https://www.jianshu.com/p/0f165dcf9525

#### 用户活跃模型表
```hql
create table tmp_liujg_dau_based(
    imp_date varchar(20) not null comment '日期',
    qimei  varchar(20) not null comment '用户唯一标识',
    is_new  varchar(10) comment '新用户表示，1表示新用户，0表示老用户',
    primary key(imp_date,qimei)
);
ALTER TABLE tmp_liujg_dau_based COMMENT '用户活跃模型表';
```
#### 红包参与领取模型表
```hql
create table tmp_liujg_packed_based  
(
    imp_date  varchar(20) comment '日期',
    report_time   varchar(20) comment '领取时间戳',
    qimei  varchar(20) not null comment '用户唯一标识',
    add_money varchar(20) not null comment '领取金额，单位为分'
);
ALTER TABLE tmp_liujg_packed_based COMMENT '红包参与领取模型表';

```

#### 问题
1、计算2019年6月1日至今，每日DAU（活跃用户量，即有登陆的用户）
```hql

```
2、计算20190601至今，每日领取红包的新用户数，老用户数，及人均领取金额，人均领取次数
```hql

```
3、计算2019年3月，每个月按领红包取天数为1、2、3……30、31天区分，计算取每个月领取红包的用户数，人均领取金额，人均领取次数
```hql

```
4、计算2019年3月，每个月领过红包用户和未领红包用户的数量，平均月活跃天数（即本月平均活跃多少天）
```hql

```
5、计算2019年3月至今，每个月活跃用户的注册日期，2019年3月1日前注册的用户日期填空即可
```hql

```
6、计算2019年3月至今，每日的用户次日留存率，领取红包用户的次日留存，未领取红包用户的次日留存率
```hql

```
7、计算2019年6月1日至今，每日新用户领取得第一个红包的金额
```hql

```
8.计算2019年3月1日至今，每个新用户领取的第一个红包和第二个红包的时间差（只计算注册当日有领取红包的用户，注册当日及以后的DAU表中新用户为1的用户）
```hql

```

