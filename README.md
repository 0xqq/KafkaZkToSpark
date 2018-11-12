# KafkaZkToSpark
###简介  
该项目提供了一个在使用spark streaming2.1+kafka0.9.0.0的版本集成时，手动存储偏移量到zookeeper中

###主要功能  

（1）提供了快速使用 spark streaming + kafka 开发流式程序的骨架，示例中的代码大部分都加上了详细的注释

（2）提供了手动管理kafka的offset存储到zookeeper的方法，并解决了一些bug，如kafka扩容分区，重启实时流不识别新增分区的问题。

（3）提供了比较简单和优雅的关闭spark streaming流式程序功能
     http端关闭脚本在 stop.sh中
     
###两者集成注意点  

1.在新版本spark streaming和kafka的集成中，按照官网的建议 spark streaming的executors的数量要和kafka的partition的个数保持相等，这样每一个executor处理一个kafka partition的数据，效率是最高的

2.createDirectStream创建流对象一旦创建就是不可变的，也就是说创建实例那一刻的分区数量，会一直使用直到流程序结束，就算中间kafka的分区数量扩展了，流程序也是不能识别到的。所以在扩展kafka分区前，一定要先把流程序给停掉，然后扩展完成后需要再次重启流程序。