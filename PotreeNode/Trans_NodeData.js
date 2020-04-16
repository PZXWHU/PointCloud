'use strict';

var ws = require("nodejs-websocket");
ws.setMaxBufferLength(1024*10);
var loggerInfo = require('./log4').logger('INFO');
var java = require('java');
var redis = require('redis');
var zlib = require('zlib');




//建立redis客户端
//detect_buffers: true  当get数据时，如果键是string，返回string，如果键是buffer，返回buffer。
var redisClient = redis.createClient('6379', '172.18.22.101',{detect_buffers: true});
//加载jar包
java.classpath.push('myHadoopUtils.jar');
//连接数据库
//java.callStaticMethod('com.pzx.HBaseUtils','init');
//console.log('连接成功!');





var server = ws.createServer(function (conn) {
    loggerInfo.info("New connection");

    conn.on("close", function (code, reason) {
        loggerInfo.info("Connection closed")
    });

    conn.on('binary', function(inStream) {
        // 创建空的buffer对象，收集二进制数据
        var request = Buffer.alloc(0);

        // 读取二进制数据的内容并且添加到buffer中
        inStream.on('readable', function() {
            var newRequest = inStream.read();
            if (newRequest)
                request = Buffer.concat([request, newRequest], request.length + newRequest.length)
        });
        inStream.on('end', function() {
            // 读取完成二进制数据后，处理二进制数据
            var result =  processRequest(conn,request)
        })
    });



    conn.on('error',(err)=>{
        loggerInfo.info("connection error！");

    })

});

function processRequest(conn,request){
    var requestType = request.readUInt8(0);
    switch (requestType) {
        case 0:
            returnTickData(conn);
            break;
        case 1:
            //获取数据的请求 type：1
            returnData1(conn,request.slice(1,request.length));
            break;
        case 2:
            //获取数据的请求 type：2
            returnData2(conn,request.slice(1,request.length));
            break;
        default:
            break;

    }
}

function returnTickData(conn) {
    let responseType = Buffer.from([0]);//responseType为0 代表心跳检测回复
    conn.sendBinary(responseType);
}

function returnData1(conn,requestBody){
    var requestJson = JSON.parse(requestBody.toString());
    var dataType = requestJson['dataType'];
    var tableName = requestJson['tableName'];
    var dataName = requestJson['dataName'];

    returnDataFromHBase1(conn,tableName,dataName,"data",dataType);

}

function returnData2(conn,requestBody){
    var requestJson = JSON.parse(requestBody.toString());
    var dataType = requestJson['dataType'];
    var tableName = requestJson['tableName'];
    var parentNodeKey = requestJson['parentNodeKey'];
    var childNodeFilterStr = requestJson['childNodeFilterStr'];

    returnDataFromHBase2(conn,tableName,"data",dataType,parentNodeKey,childNodeFilterStr);

}


function returnDataFromHBase1(conn,tableName,rowKey,colFamily,colName){

    //首先检查是否在redis中
    var bufferKey = tableName+rowKey+colName;

    //无缓存
    var time = new Date().getTime();
    var dataByteArray = java.callStaticMethodSync('com.pzx.HBaseUtils','getData',tableName,rowKey,colFamily,colName);
    if(dataByteArray ==null)
        return;
    loggerInfo.info("hbase:"+bufferKey+":"+(new Date().getTime()-time));
    //一个小时过期
    //var expireTime = 3600+Math.round(Math.random()*100);
    //redisClient.setex(bufferKey,expireTime,Buffer.from(dataByteArray));

    let responseType = Buffer.from([1]);
    let responseDataName = Buffer.from(rowKey+"."+colName,'utf-8');
    let responseDataNameLen = Buffer.from([responseDataName.byteLength]);
    conn.sendBinary(Buffer.concat([responseType,responseDataNameLen,responseDataName,zlib.gzipSync(Buffer.from(dataByteArray))]));

    /*
    redisClient.exists(bufferKey,function(err,isExisted){
        if(err)
            return;
        if(isExisted!=0){
            //有缓存
            var time = new Date().getTime();
            redisClient.get(Buffer.from(bufferKey),function(err,res){
                if(err)
                    return;
                loggerInfo.info("redis:"+bufferKey+":"+(new Date().getTime()-time));

                let responseType = Buffer.from([1]);
                let responseDataName = Buffer.from(rowKey+"."+colName,'utf-8');
                let responseDataNameLen = Buffer.from([responseDataName.byteLength]);
                conn.sendBinary(Buffer.concat([responseType,responseDataNameLen,responseDataName,zlib.gzipSync(res)]));
            })
        }else{
            //无缓存
            var time = new Date().getTime();
            var dataByteArray = java.callStaticMethodSync('com.potree.hbase.HBaseUtils','getData',tableName,rowKey,colFamily,colName);
            loggerInfo.info("hbase:"+bufferKey+":"+(new Date().getTime()-time));
            //一个小时过期
            var expireTime = 3600+Math.round(Math.random()*100);
            redisClient.setex(bufferKey,expireTime,Buffer.from(dataByteArray));

            let responseType = Buffer.from([1]);
            let responseDataName = Buffer.from(rowKey+"."+colName,'utf-8');
            let responseDataNameLen = Buffer.from([responseDataName.byteLength]);
            conn.sendBinary(Buffer.concat([responseType,responseDataNameLen,responseDataName,zlib.gzipSync(Buffer.from(dataByteArray))]));
        }
    })

     */

}


function returnDataFromHBase2(conn,tableName,colFamily,colName,parentNodeKey,childNodeFilterStr) {

    //首先检查是否在redis中
    var bufferKey = tableName+parentNodeKey+"["+childNodeFilterStr+"]"+colName;

    //无缓存
    var time = new Date().getTime();
    var dataByteArrayMap = java.callStaticMethodSync('com.pzx.HBaseUtils', 'scanWithChildNodeFilter', tableName,colFamily, colName,parentNodeKey,childNodeFilterStr);
    loggerInfo.info("hbase:" + bufferKey + ":" + (new Date().getTime() - time));

    //一个小时过期
    //var expireTime = 3600 + Math.round(Math.random() * 100);
    //redisClient.setex(bufferKey, expireTime, Buffer.from(dataByteArray));

    let responseType = Buffer.from([1]);
    var entrySetArray = dataByteArrayMap.entrySetSync().toArraySync();
    for(let index in entrySetArray){
        let rowKey  = entrySetArray[index].getKeySync();
        let vaule = entrySetArray[index].getValueSync();
        let responseDataName  = Buffer.from(rowKey.split("-")[0]+ "." + colName, 'utf-8');
        let responseDataNameLen = Buffer.from([responseDataName.byteLength]);
        conn.sendBinary(Buffer.concat([responseType, responseDataNameLen, responseDataName, zlib.gzipSync(Buffer.from(vaule))]));
    }


}




server.listen(8080);