var express = require('express');
var router = express.Router();
var esTranslation = require('../elasticsearchTransfer');


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
  var select1 = "value_count:user_id.keyword:a1" ;
  var where1 = "school_id$16,17" ;
  var source1 = "proxy.keyword:prox";
  var groupby1 = "region.keyword[5[a1:desc;province.keyword[5[a1:desc;school.keyword[5[a1:desc" ;
  app.get('/', function (req, res) {
    res.render('index', { title: 'Express' });
  });
  app.post('/getResult',function(req,res,next){
    var body = req.body ;
    console.log('********',body);
    var select = body.select||select1;
    var where = body.where||where1;
    var source = body.source||source1;
    var groupby = body.groupby||groupby1;
    var resulta = {
      "options" :{
        "metric1":{
          select1: select,
          where1: where,
          source1:source,
          groupby1: groupby
        }
      }
    }
    var result = esTranslation(resulta);
    res.send({status:result});
  });
};
