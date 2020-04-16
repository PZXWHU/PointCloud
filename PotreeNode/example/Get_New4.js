var http = require('http');
var url = require('url');
var java = require('java');
var fs = require('fs');
var zlib = require('zlib');
var loggerInfo = require('./log4').logger('INFO');
var logError = require('./log4').logger('ERROR');

java.classpath.push("/home/hlx/node_test/coprocessor.jar");
var geoDataBase = java.callStaticMethodSync("cn.edu.whu.stdb.core.geodatabase.GeoDatabase","getInstance");
java.callMethodSync(geoDataBase,"openConnection");
var instance = java.newInstanceSync("cn.edu.whu.stdb.example.query.TileQuery");

loggerInfo.info("listening...");
var visualizationQueryTime = 0;var STQueryTime = 0;var attributeQueryTime = 0;
var timeSum1 = 0;var timeSum2 = 0;var timeSum3 = 0;
var maxVisualizationQuerytime = 0;var maxSTQueryTime = 0;var maxAttributeQueryTime = 0;
var count1 = 0;var count2 = 0;var count3 = 0;
var start = new Date();

http.createServer(function(req, res){
   
   var getreq = new Date(); 
   console.log("get request"+(count1+count2+count3)+" time :"+(getreq.getTime()-start.getTime()));
   
    var params = url.parse(req.url,true).query;
	var path = url.parse(req.url,true).pathname;

	
    if(path === '/tiles'){
		var date = new Date();
		loggerInfo.info("level:" + params.level,"x:" + params.x,"y:" + params.y);
		
		var response = java.callStaticMethodSync("cn.edu.whu.stdb.example.query.TileQuery","getTSTile",parseInt(params.level), parseInt(params.y), parseInt(params.x),params.time_from,params.time_to,params.arrtibutemode,params.datasetname,geoDataBase);
		
        zlib.gzip(response.toString(), function (err, buffer) {
			if (!err) {
				res.writeHeader(200, {
					'Content-Type':'text/javascript;charset=UTF-8',
					'Content-Encoding':'gzip',
					'Access-Control-Allow-Origin': '*',
					'Access-Control-Allow-Methods': '*',
					'Access-Control-Allow-Headers': 'Origin, X-Requested-With, Content-Type, Accept'
				});
				res.write(buffer);//Buffer.from(result)
				res.end();
			}
		});
		var date1 = new Date();
		visualizationQueryTime = date1.getTime()-date.getTime();
		maxVisualizationQuerytime = visualizationQueryTime>maxVisualizationQuerytime?visualizationQueryTime:maxVisualizationQuerytime;
		timeSum1 = visualizationQueryTime + timeSum1;
		count1++;
		if(count1%500 === 0){
			console.log(timeSum1/500+'ms per VisualizationQuery');
			console.log('MaxQueryTime of 500 Vquery:'+maxVisualizationQuerytime+'ms');
			timeSum1 = 0;
			maxVisualizationQuerytime = 0;
		}
    }else if(path === '/pointQuery'){
		var date = new Date();
        var s = params.bounds.split(',');
		
        var response = java.callStaticMethodSync("cn.edu.whu.stdb.example.query.PointQuery","getSTFeature",parseFloat(s[0]), parseFloat(s[1]), parseFloat(s[2]), parseFloat(s[3]),params.time_from, params.time_to,params.datasetname,geoDataBase);
		res.writeHeader(200, {
        'Content-Type':'text/javascript;charset=UTF-8',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': '*',
        'Access-Control-Allow-Headers': 'Origin, X-Requested-With, Content-Type, Accept'
    });
		res.write(response.toString());
		res.end();
	}
	else if(path === '/tsQuery'){
		var date = new Date();
        var s = params.bounds.split(',');
		console.log("Attribute Query!");
		
        var response = java.callStaticMethodSync("cn.edu.whu.stdb.example.query.TileQuery","getTSTile",parseInt(params.level),parseFloat(s[0]), parseFloat(s[1]), parseFloat(s[2]), parseFloat(s[3]),params.time_from, params.time_to,params.arrtibutemode,parseInt(params.mode),params.datasetname,geoDataBase);
		res.writeHeader(200, {
        'Content-Type':'text/javascript;charset=UTF-8',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': '*',
        'Access-Control-Allow-Headers': 'Origin, X-Requested-With, Content-Type, Accept'
    });
		res.write(response.toString());
		res.end();
		var date1 = new Date();
        STQueryTime = date1.getTime()-date.getTime();
		maxSTQueryTime = STQueryTime>maxSTQueryTime?STQueryTime:maxSTQueryTime;
		timeSum2 = STQueryTime + timeSum2;
		count2++;
		if(count2%100 === 0){
			console.log(timeSum2/100+'ms per STQuery');
			console.log('MaxQueryTime of 100 STquery:'+maxSTQueryTime+'ms');
			timeSum2 = 0;
			maxSTQueryTime = 0;
	}
	}else if(path === '/attriquery'){
		console.log("Attribute Query!");
		var date = new Date();
		var s = params.bounds.split(',');

		var response = java.callStaticMethodSync("cn.edu.whu.stdb.example.query.TileQuery","getTSTile",parseInt(params.level),parseFloat(s[0]), parseFloat(s[1]), parseFloat(s[2]), parseFloat(s[3]),params.time_from, params.time_to,params.arrtibutemode,params.datasetname,geoDataBase);
           
		res.writeHeader(200, {
        'Content-Type':'text/javascript;charset=UTF-8',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': '*',
        'Access-Control-Allow-Headers': 'Origin, X-Requested-With, Content-Type, Accept'
    });
        res.write(response.toString());
		res.end();
		var date1 = new Date();
        attributeQueryTime = date1.getTime()-date.getTime();
		maxAttributeQueryTime = attributeQueryTime>maxAttributeQueryTime?attributeQueryTime:maxAttributeQueryTime;
		timeSum3 = attributeQueryTime + timeSum3;
		count3++;
		if(count3%100 === 0){
			console.log(timeSum3/100+'ms per AQuery');
			console.log('MaxQueryTime of 100 Aquery:'+maxAttributeQueryTime+'ms');
			timeSum3 = 0;
			maxAttributeQueryTime = 0;
	}
	}else {return}
	
	var sendres = new Date(); 
	console.log("send response"+(count1+count2+count3-1)+" time :"+(sendres.getTime()-start.getTime()));
	
}).listen(3001,'0.0.0.0',function () {
    console.log('服务器启动成功');
});



