var _ = require('lodash');
var moment = require('moment');

//terms 参数
function analyzeTerms(param,paramKey){
  var paramOption = {};
  if( !_.isEmpty(param) ){
    param = param.split(',');
    if( -1 != paramKey.indexOf('school_id') ){
      _.each( param , function(p){
        p = Number(p);
      });
    }
    var termsOption = {};
    termsOption[paramKey] = param;
    paramOption = { 'terms' : termsOption };
  }
  return paramOption;
}

//exist 参数
function analyzeExist(param,paramKey){
  var paramOption = {};
  if(1 == param ){
    paramOption = {
      "exists" : { "field" : paramKey }
    };
  }
  return paramOption;
}

//like参数
function analyzeLike(param,paramKey){
  var paramOption = {};
  if( !_.isEmpty(param) ){
    var termsOption = {}
    termsOption[paramKey] = '*' + param.toString() + '*';
    paramOption = { 'wildcard' : termsOption };
  }
  return paramOption;
}

//match参数
function analyzeMatch(param,paramKey){
  var paramOption = {};
  if( !_.isEmpty(param) ){
    var termsOption = {}
    termsOption[paramKey] = param;
    paramOption = { 'match' : termsOption };
  }
  return paramOption;
}

//tstart,tend参数,默认值2014-01-01至现在
function analyzeTime(tStart,tEnd){
  if( _.isEmpty(tStart) ){
    tStart = moment('2014-01-01T00:00:00').toDate();
  }else{
    tStart = moment(tStart).toDate();
  }
  if( _.isEmpty(tEnd) ){
    tEnd = moment().toDate();
  }else{
    tEnd = moment(tEnd).toDate();
  }
  var timeOption = {'range' : { '@timestamp' : { 'gte' : tStart,'lte' : tEnd } } };
  return timeOption;
}

//数值大于小于
function analyzeRange(param){
  var option = {};
  var rangOption = {};
  var key = '';
  var value = '';
  if( -1 != param.indexOf('#gte#') ){
    key = param.split('#gte#')[0];
    value = param.split('#gte#')[1];
    rangOption[key.toString()] = {'gte': value} ;
    option = {'range' : rangOption };
  }else if( -1 != param.indexOf('#lte#') ){
    key = param.split('#lte#')[0];
    value = param.split('#lte#')[1];
    rangOption[key.toString()] = {'lte': value} ;
    option = {'range' : rangOption };
  }else if( -1 != param.indexOf('#gt#') ){
    key = param.split('#gt#')[0];
    value = param.split('#gt#')[1];
    rangOption[key.toString()] = {'gt': value} ;
    option = {'range' : rangOption };
  }else if( -1 != param.indexOf('#lt#') ){
    key = param.split('#lt#')[0];
    value = param.split('#lt#')[1];
    rangOption[key.toString()] = {'lt': value} ;
    option = {'range' : rangOption };
  }
  return option;
}

//拼写filterAggs
function getFilterAggs(whereOption){
  var whereParams = whereOption.split(';');
  var option = {};
  var mustArray = [];
  var result = {};
  // console.log(whereParams);
  whereParams = _.compact(whereParams);
  _.each( whereParams,function(w){
    if( -1 != w.indexOf(':') ){
      var key = w.split(':')[0];
      var whereArray = w.split(':');
      whereArray.shift();
      var value = whereArray[0];
      if(whereArray.length>1){
        value = whereArray.join(":");
      }
      option = analyzeMatch(value,key);
    }
    if( -1 != w.indexOf('?') ){
      var key = w.split('?')[0];
      var value = w.split('?')[1];
      option = analyzeExist(value,key);
    }
    if( -1 != w.indexOf('>') ){
      var key = w.split('>')[0];
      var value = w.split('>')[1];
      option = analyzeTime(value,'');
    }
    if( -1 != w.indexOf('<') ){
      var key = w.split('<')[0];
      var value = w.split('<')[1];
      option = analyzeTime('',value);
    }
    if( -1 != w.indexOf('$') ){
      var key = w.split('$')[0];
      var value = w.split('$')[1];
      option = analyzeTerms(value,key);
    }
    if( -1 != w.indexOf('%') ){
      var key = w.split('%')[0];
      var value = w.split('%')[1];
      option = analyzeLike(value,key);
    }
    if( -1 != w.indexOf('#gte#') ||
        -1 != w.indexOf('#lte#') ||
        -1 != w.indexOf('#gt#') ||
        -1 != w.indexOf('#lt#') ){
      option = analyzeRange(w);
    }
    if( !_.isEmpty(option) ){
      mustArray.push(option);
    }
  });
  result = {
    "bool":{
      "must":mustArray
    }
  };
  return result;
}

//拼写所有聚合
function getAllAggs(selectOption,sourceOption){
  var result = {};
  var selectors = selectOption.split(';');
  selectors = _.compact(selectors);
  _.each( selectors,function(s){
    var name = s.split(':')[2];
    var aggs = s.split(':')[0];
    var field = s.split(':')[1];
    var aggSub = {}
    aggSub[aggs.toString()] = { "field" : field };
    result[name.toString()] = aggSub;
  });
  if( !_.isEmpty(sourceOption) ){
    var sources = sourceOption.split(';');
    _.each(sources,function(sou){
      var name = sou.split(':')[1];
      var field = sou.split(':')[0];
      var size = 50000;
      var sourceTerm = {
        "terms": {
          "field": field,
          "size": size
        }
      };
      result[name.toString()] = sourceTerm;
    });
  }
  return result;
}

//组装一个agg供排序用
function getSortAggs(orderContent,innerAgg,sortFlag){
  if(sortFlag==0){
    return  innerAgg[orderContent.toString()];
  }else{
    var ls_agg = innerAgg;
    for(var j =0;j<=sortFlag;j++){
      // console.log( orderContent,'--',sortFlag,'----',innerAgg,'---',JSON.stringify(ls_agg) );
      if(j<sortFlag){
        ls_agg = ls_agg['innerAgg1']['aggs'];
      }else{
        ls_agg = ls_agg[orderContent.toString()];
      }
    }
    return ls_agg;
  }
}

//group by基本组装
function getGroupBy(groupbyOption,innerAgg,sortFlag){
  var agg= {};
  if( '0' == groupbyOption ){
    agg = innerAgg;
  }else if( -1 != groupbyOption.indexOf('[') ){
    var orders = groupbyOption.split('[')[2];
    var by = 'orderAgg';
    var order = orders.split(':')[1]||"asc";
    var orderContent = orders.split(':')[0];
    var orderAggs = getSortAggs(orderContent,innerAgg,sortFlag);
    innerAgg['orderAgg'] = orderAggs;
    var orderOption = {};
    orderOption[by.toString()] = order;
    var topn = groupbyOption.split('[')[1];
    var groupbyField = groupbyOption.split('[')[0];
    agg = {
      "terms" : { "field" : groupbyField, "order" : orderOption,"size" : topn },
      "aggs" : innerAgg
    };
  }else if( -1 != groupbyOption.indexOf('(') ){
    var groupbyField = groupbyOption.split('(')[0];
    var interval = groupbyOption.split('(')[1];
    agg = {
      "date_histogram" : { "field" : groupbyField, "interval" : interval, "format" : "yyyy-MM-dd HH:mm:ss","time_zone": "+08:00" },
      "aggs" : innerAgg
    };
  }
  return agg;
}

//一次group
function getOnceOuterAggs(groupbyOption,innerAgg){
  var groupbyAgg = getGroupBy(groupbyOption,innerAgg,0);
  return {
    "innerAgg" : groupbyAgg
  };
}

//两次group
function getDoubleOuterAggs(groupbyOption,innerAgg,selectOption,flag){
  var groups = groupbyOption.split(';');
  var group0 = {};
  var aggSub = {};
  var agg0 = {};
  var agg1 = {};
  var sortFlag = 0;
  groups = _.compact(groups);
  for(var i=groups.length-1;i>=0;i--,sortFlag++){
    group0 = groups[i];
    agg0 = getGroupBy(group0,innerAgg,sortFlag);
    if(i != 0){
      if( -1 != group0.indexOf('[') ){
        aggSub = agg0.aggs.orderAgg;
        agg1['innerAgg'+flag.toString()] = agg0;
      }else if( -1 != group0.indexOf('(') ){
        var name = selectOption.split(':')[2];
        aggSub = agg0.aggs[name.toString()];
        agg1['innerAgg'+flag.toString()] = agg0;
      }
    }else{
      agg1 = agg0;
    }
    innerAgg = agg1;
    agg1 = {};
  }
  return {
    "innerAgg1" : innerAgg
  };
}


//聚合
function agg_action(body){
  var options = body.options;
  console.log('请求体:',JSON.stringify(options) );

  var aggsOption = {};
  var flag = 1;
  _.each(options,function(option){
    var select = 'select'+flag;
    var where = 'where'+flag;
    var groupby = 'groupby'+flag;
    var orderby = 'orderby'+flag;
    var source = 'source'+flag;
    var metricString = 'metric'+flag;
    var metric = option;
    var selectOption = metric[select.toString()];
    var whereOption = metric[where.toString()];
    var groupbyOption = metric[groupby.toString()];
    var orderbyOption = metric[orderby.toString()];
    var sourceOption = metric[source.toString()];
    var filterAggs = getFilterAggs(whereOption);
    var selectAggs = getAllAggs(selectOption,sourceOption);
    var innerAgg = {};
    innerAgg = selectAggs ;

    //区分一次groupby和两次groupby
    var outerAggs = {};
    if( -1 == groupbyOption.indexOf(';') ){
      outerAggs = getOnceOuterAggs(groupbyOption,innerAgg)
    }else {
      outerAggs = getDoubleOuterAggs(groupbyOption,innerAgg,selectOption,flag);
    }
    if( '0' == groupbyOption ){
      aggsOption[metricString.toString()] = {
        "filter" : filterAggs,
        "aggs" : outerAggs.innerAgg
      };
    }else{
      finalAggs = {
        "filter" : filterAggs,
        "aggs" : outerAggs
      };
      aggsOption[metricString.toString()] = finalAggs;
    }
    flag++;
  });
  console.log('查询中的aggs: ', JSON.stringify(aggsOption) );
  //最终的查询条件,聚合条件
  var searchOption = {
    'index': ['boss-*'],
    'body': {
      'size' : 0,
      "aggs": aggsOption
    }
  };
  return aggsOption;
}


//搜索
function search_detail(body){
  //处理参数
  var sort = body.sort;
  var source = body.source;
  var pageNum = body.pageNum;
  var page = body.page;
  var queryOption = getFilterAggs(body.where);

  var bodyOption = {};
  var sortOption = [];
  //排序sort
  if( !_.isEmpty(sort) ){
    var sorts = sort.split(',');
    _.each( sorts , function(option){
      var key = option.split(':')[0];
      var value = option.split(':')[1];
      var sortElement = {};
      sortElement[key] = value;
      sortOption.push( sortElement );
    });
  }
  //source 你要查询的字段
  if( !_.isEmpty(source) ){
    source = source.split(',');
  }else{
    source = ['school_id'];
  }

  bodyOption['query'] = queryOption;
  bodyOption['sort'] = sortOption;
  bodyOption['size'] = pageNum;
  //scroll 查询条件
  var scrollOption = {
    'index': 'boss-*',
    'scroll': '10s',
    '_source': source,
    'body': bodyOption
  }
  console.log('条件  :',JSON.stringify(scrollOption.body.query.bool.must) );

  // page<0 获取全部数据
  if( 0 > page ){
    return scrollOption;
  }
  bodyOption['query'] = queryOption;
  bodyOption['sort'] = sortOption;
  var fromNum =  (page-1)*pageNum
  bodyOption['from'] = fromNum;
  bodyOption['size'] = pageNum;
  //scroll 查询条件
  scrollOption = {
    'index': 'boss-*',
    '_source': source,
    'body': bodyOption
  };
  console.log('条件  :',JSON.stringify(scrollOption.body.query.bool.must));
  return scrollOption;
}

// var a = {"options":{"metric1":{"select1":"value_count:province.keyword:c_p;sum:order_id:s_o;","where1":"_type:table_a;b>2018-01-02;c%ccc;","groupby1":"test_time(day;b[199[s_o;"},"metric2":{"select2":"value_count:city.keyword:c_c;sum:amount:s_a;","where2":"_type:table_b;b_0>2018-01-02;c_0%ccc;","groupby2":"test_time_1(month;i[300[s_a;"}}};
// agg_action(a);
module.exports = agg_action;
