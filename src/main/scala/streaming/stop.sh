#!/bin/bash
## tx.log是提交spark任务后的输出log重定向的log
## &> tx.log &
driver=`cat tx.log | grep ApplicationMaster | grep -Po '\d+.\d+.\d+.\d+'`

echo $driver

curl http://$driver:port/close/

echo "stop finish"
