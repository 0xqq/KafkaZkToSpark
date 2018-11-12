package streaming

import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
  * Created by Administrator on 2017/12/18.
  */
object KafkaOffsetManager {
  lazy val log = org.apache.log4j.LogManager.getLogger("KafkaOffsetManage")

  /**
    * 读取zk里面的偏移量,如果有就返回对应的分区和偏移量
    * 没有就返回None
    * @param zkClient  zk连接的client
    * @param zkOffsetPath 偏移量的路径
    * @param topic topic 名字
    * @return 偏移量 map or none
    */
  def readOffsets(zkClient : ZkClient,zkOffsetPath : String, topic : String) : Option[Map[TopicAndPartition,Long]]= {

      //从zk上读取偏移量
      val (offsetsRangesStrOpt,_) = ZkUtils.readDataMaybeNull(zkClient,zkOffsetPath)

      //匹配
    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        //这个topic在zk里面最新的分区数量
        val lastset_partitions = ZkUtils.getPartitionsForTopics(zkClient,Seq(topic)).get(topic).get
        log.warn("zk最新分区信息"+offsetsRangesStr.toString)
        //zookeeper 中数据[String:String,String:String]
        var offsets = offsetsRangesStr.split(",") //按逗号split成数组
          .map(s => s.split(":")) //按冒号拆分每个分区和偏移量
          .map { case Array(partitionStr, offsetStr) => TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong } //加工成最终的格式
          .toMap //返回一个map

        //说明有分区扩展了
        if(offsets.size < lastset_partitions.size){
          //得到旧的所有分区序号
          val old_partitions = offsets.keys.map(p => p.partition).toArray
          //通过做差集的出来的分区数量数组
          val add_partitions = lastset_partitions.diff(old_partitions)
          if(add_partitions.nonEmpty){
            log.info("发现Kafka新增的分区"+ add_partitions.mkString(","));
            add_partitions.foreach(partitionId => {
              //从0消费
              offsets += (TopicAndPartition(topic,partitionId) -> 0)
              log.warn("新增分区id："+partitionId+"添加完毕....")
            })
          }


        }else{
          log.warn("没有发现新增的kafka分区："+lastset_partitions.mkString(","))
        }
        Some(offsets)//将Map返回

      case None =>
        None

    }
  }

  /**
    * 保存每个批次的rdd的offset到zk中
    * @param zkClient zk连接的client
    * @param zkOffsetPath 偏移量路径
    * @param rdd 每个批次的Rdd
    */
  def saveOffsets(zkClient:ZkClient,zkOffsetPath:String,rdd:RDD[_]) : Unit = {
    //转换rdd为Array[OffsetRange]
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    log.debug(" 保存前的偏移量：  "+offsetRanges.toString)
    //转换每个offsetRanges为存储到zk时的字符串格式 : 分区号1:偏移量,分区号2:偏移量,...
    val offsetsRangesStr = offsetRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}").mkString(",")
    log.debug(" 保存的偏移量：  "+offsetsRangesStr)
    //将最终的字符串结果保存在zk里面
    ZkUtils.updatePersistentPath(zkClient,zkOffsetPath,offsetsRangesStr)
  }


  class Stopwatch {
    private val start = System.currentTimeMillis()
    def get():Long = (System.currentTimeMillis() - start)
  }
}
