本节使用的表有： 

(1) user_info表  
字段有用户ID、姓名、性别、年龄、城市、首次激活时间、等级、两个额外字段。  
```hql
user_id     string
user_name   string
sex         string
age         int
city        string
firstActiveTime string
level       string
extra1      string
extra2      map<string,string>
```
字段extra1(string)示例：{"systemtype": "android", "education": "doctor", "marriage_status": "1", "phonebrand": "VIVO"};  
字段extra2(map)示例：systemtype:android,education:doctor,marriage_status:1,phonebrand:VIVO  

(2) user_trade表  
字段有：用户姓名、购买数量、价格、支付金额、商品分类、支付时间（时间戳格式）、分区日期
tips：看到partition 应该注意到，这是个分区表，在查询的时候需要对dt进行限定。  
```hql
user_name               string  //用户姓名
piece                   int   //购买数量
price                   double //价格 
pay_amount              double    //支付金额
goods_category          string //商品分类
pay_time                bigint //支付时间（时间戳格式）
dt                      string //分区日期
```