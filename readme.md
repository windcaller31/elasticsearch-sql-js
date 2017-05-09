{
  options : 固定查询格式如下

    {
      "metric1":{
        "select1": "cardinality:user_id.keyword:a1;sum:amount:a2" ,
        "where1" : "school_id$16,17" ,
        "source1": "proxy.keyword:prox",
        "groupby1" : "school.keyword[5[a2:desc"
      },
      "metric2":{
        "select2": "value_count:user_id.keyword:a1" ,
        "where2" : "school_id$16,17" ,
        "groupby2" : "province.keyword[5[a1:desc;school.keyword[5[a1:desc"
      }
    }
}
    1.使用限制
    每个metric支持一个group by对应多个聚合 ,或者 一个聚合对应2个group by
    多个聚合(N个一对一聚合和group by)对应多个group by 应该分开写多个metric

    2.语法说明

  =>select
    value_count:user_id.keyword:a1 ; sum:amount:a2
    用';'分割不同的聚合,聚合内部用':'分割
    1.
    cardinality  : unique_count去重计数
    sum   : sum求和
    value_count : count不去重计数

    2.
    user_id.keyword
    聚合的对象 count(user_id.keyword)

    3.
    a1是这个聚合的别名

  =>where
    不同的where条件用';'分割

    : 表示 等于                 province.keyword:山西
    ? 表示 存在 0不存在 1存在     parent_id.keyword?1
    > 表示 tStart大于           tStart>2017-01-14 00:00:00
    < 表示 tEnd小于             tEnd>2017-01-14 00:00:00
    $1,2,3 表示 in[1,2,3]      school_id$16,17
    %a 表示 like %a%           school.keyword%'山'

  =>groupby

    province.keyword[5[a1:desc ; @timestamp(day

    1.
    province.keyword[5[a1:desc
    [ : 带'['表示terms
    province.keyword : 表示 group by province.keyword  
    5 : top5
    a1:desc  按照聚合结果名字为a1的desc顺序排序 desc降序 asc升序

    2.
    @timestamp(day
    ( : 带'('表示按时间分组
    @timestamp : @timestamp目前就固定按照这个时间字段分组
    day : 分组级别 year, quarter, month, week, day, hour, minute, second

    0:
    不按任何情况分组，去聚合的一个值

  => source
    表示在查询统计时候需要夹带的明细信息
    proxy.keyword:prox
    proxy.keyword表示字段   prox  表示给字段起的别名
# es-js
