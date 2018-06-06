select count(province.keyword) as c_p , sum(order_id) as s_o from index_a.table_a where a:1 and b>2018-01-02 and c%ccc group by to_date(test_time,day) , b:199 union select count(city.keyword) as c_c , sum(amount) as s_a from index_b.table_b where a_0:1 and b_0>2018-01-02 and c_0%ccc group by to_date(test_time_1,month) , i:300

类似sql语法
select ... from ... where ... group by  ...  union
as and 全部小写
时间分组 to_date(a,day)  year  quarter  month  week  day  hour  minute  second

统计操作符只有下述几种:
  "count" : "value_count",
  "sum" : "sum",
  "avg" : "avg",
  "cardinality" : "cardinality", //count(distinct(xxx))
  "max" : "max",
  "min" : "min"

比较操作符只有下述几种:
  "=" : ":",    // =
  "?" : "?",    // 0:is null  1: is not null
  ">" : ">",    // time >
  "<" : "<",    // time <
  "$" : "$",    // in[]
  "%" : "%"     // like *a*
