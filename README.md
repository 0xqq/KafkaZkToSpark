# KafkaZkToSpark
###简介
该项目提供了一个在使用spark streaming2.1+kafka0.9.0.0的版本集成时，手动存储偏移量到zookeeper中

###主要功能

（1）提供了快速使用 spark streaming + kafka 开发流式程序的骨架，示例中的代码大部分都加上了详细的注释

（2）提供了手动管理kafka的offset存储到zookeeper的方法，并解决了一些bug，如kafka扩容分区，重启实时流不识别新增分区的问题。

（3）提供了比较简单和优雅的关闭spark streaming流式程序功能
     http端关闭脚本在 stop.sh中