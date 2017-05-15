# mine
npm start 启动
访问 localhost:3000




用js实现的elasticsearch查询,把我自己定义的一种类似sql的语句翻译成elasticsearch的请求体


var resulta = {
  "options" :{
    "metric1":{
      "select1": "value_count:user_id.keyword:a1" ,
      "where1" : "school_id$16,17" ,
      "groupby1" : "region.keyword[5[a1:desc;province.keyword[5[a1:desc;school.keyword[5[a1:desc"
    }
  }
};
agg_action(resulta);
调用agg_action 完成你的梦想
详细支持见readme.md
