'use strict';

var ws = require("nodejs-websocket");
ws.setMaxBufferLength(1024*10);
var loggerInfo = require('./log4').logger('INFO');

var zlib = require('zlib');




var server = ws.createServer(function (conn) {
    loggerInfo.info("New connection");
    console.log(conn['key']);

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
            loggerInfo.info(request)
            console.log(conn['key'])
            returnTickData(conn)
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
            returnData(conn,request.slice(1,request.length));
            break;
        default:
            break;

    }
}

function returnTickData(conn) {
    let responseType = Buffer.from([0]);//responseType为0 代表心跳检测回复
    conn.sendBinary(responseType);
}

function returnData(conn,requestBody){
    var requestJson = JSON.parse(requestBody.toString());
    var dataType = requestJson['dataType'];
    var tableName = requestJson['tableName'];
    var dataName = requestJson['dataName'];

    returnDataFromHBase(conn,tableName,dataName,"data",dataType);

}


function returnDataFromHBase(conn,tableName,rowKey,colFamily,colName){

    //首先检查是否在redis中
    var bufferKey = tableName+rowKey+colName;
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

}

/*
function sendResponseToConnection(responseTypeInt,responseHeaderJson,responseBodyBuffer) {
    let responseTypeBuffer = Buffer.alloc(1);
    responseBodyBuffer.setUint8(0,1);
    let responseHeaderBuffer = Buffer.from(JSON.stringify(responseHeaderJson),'utf-8');
    let responseHeaderLen = responseHeaderBuffer.byteLength;
    let responseBodyLen = responseBodyBuffer.byteLength;

}


function sendBinaryToConnection1(conn, ...values){
    var outputStream = conn.beginBinary();
    for(let i=0;i<values.length;i++){
        outputStream.write(values)
    }
    outputStream.end;
}

 */

server.listen(8080);






