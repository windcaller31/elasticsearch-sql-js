var express = require('express');
var router = express.Router();
var esTranslation = require('../elasticsearchTransfer');
var transfer_outer_sql = require('../sqlTransfer');

module.exports = function(app) {
  // var resulta = {
  //     "options" :{
  //       "metric1":{
  //         "select1": "value_count:user_id.keyword:a1" ,
  //         "where1" : "school_id$16,17" ,
  //         "groupby1" : "region.keyword[5[a1:desc;province.keyword[5[a1:desc;school.keyword[5[a1:desc"
  //       }
  //     }
  // };
  // var select1 = "value_count:user_id.keyword:a1" ;
  // var where1 = "school_id$16,17" ;
  // var source1 = "proxy.keyword:prox";
  // var groupby1 = "region.keyword[5[a1:desc;province.keyword[5[a1:desc;school.keyword[5[a1:desc" ;
  var test_sql = "select count(province.keyword) as c_p , sum(order_id) as s_o from index_a.table_a where a:1 and b>2018-01-02 and c%ccc group by to_date(test_time,day) , b:199";
  test_sql += " union select count(city.keyword) as c_c , sum(amount) as s_a from index_b.table_b where a_0:1 and b_0>2018-01-02 and c_0%ccc group by to_date(test_time_1,month) , i:300"
  app.get('/', function (req, res) {
    res.render('index', { title: 'Express' });
  });
  app.post('/getResult',function(req,res,next){
    var body = req.body ;
    var _sql = body.sql||test_sql;
    var mid_result = transfer_outer_sql(_sql);
    console.log(JSON.stringify(mid_result));
    var result = esTranslation(mid_result);
    // console.log('********',body);
    // var select = body.select||select1;
    // var where = body.where||where1;
    // var source = body.source||source1;
    // var groupby = body.groupby||groupby1;
    // var resulta = {
    //   "options" :{
    //     "metric1":{
    //       select1: select,
    //       where1: where,
    //       source1:source,
    //       groupby1: groupby
    //     }
    //   }
    // }
    // var result = esTranslation(resulta);
    res.send({status:result});
  });
};
