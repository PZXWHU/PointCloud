'use strict';

//导入各模块
var fs = require('fs');
var url = require('url');
var path = require('path');
var http = require('http');
var java = require('java');
var redis = require('redis');
var loggerInfo = require('./log4').logger('INFO');



var colFamily = 'data';
var lasColName = 'las';
var hrcColName = 'hrc';
var jsColName = 'js';
var binColName = 'bin'
var lazColName = 'laz'

/**将查询字符串转化为查询map */
function getQueryMap(queryStr){
    var queryMap = {};
    if(queryStr!=null){
        var queryArray = queryStr.split("&");
        for(let queryItem of queryArray){
            queryMap[queryItem.split("=")[0]] = queryItem.split("=")[1];
        }
    }
    return queryMap;
}

//像response返回数据
function returnResponseData(response,data){
    response.writeHead(200,{'Content-Type' : 'application/octet-stream','Access-Control-Allow-Origin':'*'});
    response.end(data);
}


function readFromRedis(bufferKey,tableName,nodeName,colName,response) {

    var nodeDataByteArray = java.callStaticMethodSync('com.pzx.HBaseUtils','getData',tableName,nodeName,colFamily,colName);

    returnResponseData(response,Buffer.from(nodeDataByteArray));

    /*
    redisClient.exists(bufferKey,function(err,isExisted){
        if(err)
            return;
        if(isExisted!=0){
            //有缓存
            var time = new Date().getTime()
            redisClient.get(Buffer.from(bufferKey),function(err,res){
                if(err)
                    return;
                loggerInfo.info("redis:"+bufferKey+":"+(new Date().getTime()-time))
                returnResponseData(response,res)
            })
        }else{
            //无缓存
            var time = new Date().getTime()
            var nodeDataByteArray = java.callStaticMethodSync('com.potree.hbase.HBaseUtils','getData',tableName,nodeName,colFamily,colName);
            loggerInfo.info("hbase:"+bufferKey+":"+(new Date().getTime()-time))
            //一个小时过期
            var expireTime = 3600+Math.round(Math.random()*100);
            redisClient.setex(bufferKey,expireTime,Buffer.from(nodeDataByteArray));
            returnResponseData(response,Buffer.from(nodeDataByteArray));

        }
    })

     */

}

//建立redis客户端
//detect_buffers: true  当get数据时，如果键是string，返回string，如果键是buffer，返回buffer。
//var redisClient = redis.createClient('6379', '172.18.22.101',{detect_buffers: true});
//加载jar包
java.classpath.push('myHadoopUtils.jar');

var server = http.createServer(function(request,response){
    // 回调函数接收request和response对象,
    
    //访问url的pathname为PotreeView时才能获取数据
    var urlJson = url.parse(request.url);
    if(urlJson.pathname.indexOf("/pointCloudData")==-1){
        response.writeHead(200,{'Content-Type' : 'text/html;charset=UTF-8'});
        response.end('<h1>访问错误</h1>');
        return;
    }

    var pathArray = urlJson.pathname.split("/");//pathArray[0]=""
    var tableName = pathArray[2]
    var queryMap = getQueryMap(urlJson.query);

    if('lasName' in queryMap){
        
        //获取点云分片数据
        var lasName = queryMap['lasName']
        var bufferKey = tableName+lasName+lasColName;
        readFromRedis(bufferKey,tableName,lasName,lasColName,response)

    }else if ('binName' in queryMap){
        //获取点云分片数据
        var binName = queryMap['binName']
        var bufferKey = tableName+binName+binColName;
        readFromRedis(bufferKey,tableName,binName,binColName,response)

    } else if('hrcName' in queryMap){
        //获取索引文件数据
        var hrcName = queryMap['hrcName'];
        var hrcDataByteArray = java.callStaticMethodSync('com.pzx.HBaseUtils','getData',tableName,hrcName,colFamily,hrcColName);
        returnResponseData(response,Buffer.from(hrcDataByteArray));

    }else if('jsName' in queryMap){
        //获取点云cloudjs数据
        var jsName = queryMap['jsName'];
        var jsDataByteArray = java.callStaticMethodSync('com.pzx.HBaseUtils','getData',tableName,jsName,colFamily,jsColName);
        returnResponseData(response,Buffer.from(jsDataByteArray));
    }
    

});

// 让服务器监听3000端口:
server.listen(8087);

console.log('Server is running at http://127.0.0.1:8087/');