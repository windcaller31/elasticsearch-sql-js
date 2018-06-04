var _ = require('lodash');
// var select1 = "value_count:user_id.keyword:a1" ;
// var where1 = "school_id$16,17" ;
// var source1 = "proxy.keyword:prox";
// var groupby1 = "region.keyword[5[a1:desc;province.keyword[5[a1:desc;school.keyword[5[a1:desc" ;
// select ... from ... where ... group by  ...  union
//  as and 全部小写
// 时间分组 to_date(a,'day')  year  quarter  month  week  day  hour  minute  second
var select_enum = {
  "count" : "value_count",
  "sum" : "sum",
  "avg" : "avg",
  "cardinality" : "cardinality",
  "max" : "max",
  "min" : "min"
};

var where_op_enum = {
  "=" : ":",    // =
  "?" : "?",    // 0:is null  1: is not null
  ">" : ">",    // time >
  "<" : "<",    // time <
  "$" : "$",    // in[]
  "%" : "%"     // like *a*
};

function remove_blank(s){
  return s.replace(/\s+/g,'');
}

function split_by_kuohao(s){
  var linshi_s = s.split(/\(|\)/);
  return _.compact(linshi_s);
}

function transfer_outer_sql(sql){
  var outer_sql = [];
  outer_sql = sql.split("union");
  var final_result = {};
  var index_array = [];
  for(var i=0;i<outer_sql.length;i++){
    var inner_sql = outer_sql[i];
    var index = "";
    var inner_sql_obj = transfer_inner_sql(inner_sql,index)
    var metric_key = "metric" + (i+1);
    var select_key = "select" + (i+1);
    var where_key = "where" + (i+1);
    var group_key = "groupby" + (i+1);
    final_result[metric_key] = {};
    final_result[metric_key][select_key] = inner_sql_obj["select"];
    final_result[metric_key][where_key] = inner_sql_obj["where"];
    final_result[metric_key][group_key] = inner_sql_obj["groupby"];
    index_array.push(index);
  }
  console.log(index_array);
  return final_result;
}

function transfer_inner_sql(inner_sql,index){
    inner_sql = inner_sql.split(" ");
    var select_str = '';
    var where_str = '';
    var group_str = '';
    var sql_stack = [];
    var return_order_select = '';
    for(var j =0;j<inner_sql.length;j++){
      if((inner_sql[j]!='select')
      &&(inner_sql[j]!='from')
      &&(inner_sql[j]!='where')
      &&(inner_sql[j]!='group')
      &&(inner_sql[j]!='by')){
        sql_stack.push(inner_sql[j]);
      }
      if(inner_sql[j]=='from'){
        // {
        //   "select" : return_select,
        //   "order" : return_order_select
        // }
        select_obj = transfer_select(sql_stack);
        select_str = select_obj.select;
        return_order_select = select_obj.order;
        sql_stack = [];
      }
      if(inner_sql[j]=='where'){
        // {
        //   "index" : s[0],
        //   "where_str" : where_str
        // };
        var from_obj = transfer_from(sql_stack,index);
        index = from_obj.index;
        where_str += from_obj.where_str;
        sql_stack = [];
      }
      if(inner_sql[j]=='group'){
        where_str += transfer_where(sql_stack);
        sql_stack = [];
      }
    }
    if(sql_stack.length>0){
      group_str = transfer_group(sql_stack,return_order_select);
    }
    return {
      "select" : select_str,
      "where" : where_str,
      "groupby" : group_str
    };
}


function transfer_select(s){
  var each_stack = [];
  var select_final = [];
  var return_select = "";
  var return_order_select = "";
  for(var i =0;i<s.length;i++){
    var si = s[i];
    if(si == ","){
      var each_mid_select = deal_each_select(each_stack);
      return_order_select = each_mid_select.split(":")[each_mid_select.split(":").length-1]
      return_select += each_mid_select;
      each_stack = [];
    }
    each_stack.push(si);
  }
  return {
    "select" : return_select,
    "order" : return_order_select
  };
}

function transfer_from(s,index){
  var s_0 = s[0];
  s = s_0.split("\.");
  var where_str = "";
  where_str += "_type:";
  where_str += s[1];
  where_str += ";";
  return {
    "index" : s[0],
    "where_str" : where_str
  };
}

function transfer_where(s){
  s = s.split("and");
  var where_str = "";
  for(var i =0;i<s.length;i++){
    var si = s[i];
    where_str += deal_each_where(si);
  }
  return where_str;
}

function transfer_group(s,return_order_select){
  s = s.split(",");
  var group_str = "";
  for(var i =0;i<s.length;i++){
    var si = s[i];
    group_str += deal_each_group(s,return_order_select)
  }
  return group_str;
}

function deal_each_select(each_stack){
  var each_select_str = "";
  var each_stack_array = each_stack.split('as');
  var esa_0 = each_stack_array[0];
  var esa_1 = each_stack_array[1];
  var new_esa_0 = remove_blank(esa_0);//count(a) 计算方式
  var new_esa_1 = remove_blank(esa_1);//count_a  计算字段的名字
  new_esa_0_array = split_by_kuohao(new_esa_0);
  var agg_op = new_esa_0_array[0];
  agg_op = select_enum[agg_op];
  var agg_col = new_esa_0_array[1];
  each_select_str += agg_op;
  each_select_str += ":";
  each_select_str += agg_col;
  each_select_str += ":";
  each_select_str += new_esa_1;
  each_select_str += ";";
  return each_select_str;
}

function deal_each_where(each_stack){
  var each_where_str = "";
  if(each_stack.indexOf("=")!=-1){
    var sub_str = each_stack.split("=");
    each_where_str+= sub_str[0];
    each_where_str+= "=";
    each_where_str+= sub_str[1];
    each_where_str+= ";";
  }
  if(each_stack.indexOf("?")!=-1){
    var sub_str = each_stack.split("?");
    each_where_str+= sub_str[0];
    each_where_str+= "?";
    each_where_str+= sub_str[1];
    each_where_str+= ";";
  }
  if(each_stack.indexOf(">")!=-1){
    var sub_str = each_stack.split(">");
    each_where_str+= sub_str[0];
    each_where_str+= ">";
    each_where_str+= sub_str[1];
    each_where_str+= ";";
  }
  if(each_stack.indexOf("<")!=-1){
    var sub_str = each_stack.split("<");
    each_where_str+= sub_str[0];
    each_where_str+= "<";
    each_where_str+= sub_str[1];
    each_where_str+= ";";
  }
  if(each_stack.indexOf("$")!=-1){
    var sub_str = each_stack.split("$");
    each_where_str+= sub_str[0];
    each_where_str+= "$";
    each_where_str+= sub_str[1];
    each_where_str+= ";";
  }
  if(each_stack.indexOf("%")!=-1){
    var sub_str = each_stack.split("%");
    each_where_str+= sub_str[0];
    each_where_str+= "%";
    each_where_str+= sub_str[1];
    each_where_str+= ";";
  }
  return each_where_str;
}

//@timestamp(day
//province.keyword[5[a1:desc
function deal_each_group(s,return_order_select){
  var group_str = "";
  if(s.indexOf(":")){
    s = s.split(":");
    group_str += s[0];
    group_str += "[";
    group_str += s[1];
    group_str += "[";
    group_str += return_order_select;
  }else{
    group_str += s;
    group_str += "[1000";//默认top100
    group_str += "[";
    group_str += return_order_select;
  }
  if(s.indexOf("to_date")){
    var linshi_s = s.split(/to_date\(|\)|,|'/);
    linshi_s = _.compact(linshi_s);
    group_str += linshi_s[0];
    group_str += "(";
    group_str += linshi_s[1];
  }
  group_str += ";";
  return group_str;
}

var test_sql = "select count(province.keyword) from index_a.table_a where a:1 and b>2018-01-02 and c%ccc group by to_date(test_time,day),b:199";
console.log(JSON.stringify(transfer_outer_sql(test_sql)));
