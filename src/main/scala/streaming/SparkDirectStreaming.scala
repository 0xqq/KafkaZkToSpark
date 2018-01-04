package streaming

import kafka.api.OffsetRequest
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.sql.Connection

import org.apache.log4j.{Level, Logger}

/**
  * 参考github: https://github.com/qindongliang
  * 优雅的停止spark任务
  * Created by Administrator on 2017/12/18.
  */
object SparkDirectStreaming {
  val log = org.apache.log4j.LogManager.getLogger("SparkDirectStreaming")
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


  def main(args: Array[String]): Unit = {
    //创建StreamingContext
    val ssc = createStreamingContext()
    //开始执行
    ssc.start

    //方式一:根据扫描文件关闭
    stopByMarkFile(ssc)


    ssc.awaitTermination()


  }

  def stopByMarkFile (ssc:StreamingContext) : Unit = {
    //每隔十秒10秒扫描一个消息是否存在
    val intervalMills = 10*1000
    var isStop = false
    val hdfs_file_path = "/user/flume/Test/stop"
    while(!isStop){
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if(!isStop && isExistsMarkFile(hdfs_file_path)){
        log.warn("2秒后开始关闭sparstreaming程序.....")
        Thread.sleep(2000)
        ssc.stop(true, true)
      }
    }


  }

  def isExistsMarkFile(hdfsFlie : String) :Boolean ={
    val conf = new Configuration()
    val path = new Path(hdfsFlie)
    val fs = path.getFileSystem(conf)
    fs.exists(path)
  }

  /**
    * 创建StreamingContext
    *
    * @return
    */
  def createStreamingContext(): StreamingContext = {

    //是否使用local模式
    val isLocal = true

    //第一次启动是否从最新的offset开始消费
    val firstReadLastest = true

    val sparkConf = new SparkConf().setAppName("Direct Kafka Offset to Zookeeper")

    if (isLocal) {
      sparkConf.setMaster("local[2]")
    }


    //配置spark流信息

    //优雅的关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    //激活最佳消费速率
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    //第一次读取的最大数据值
    sparkConf.set("spark.streaming.backpressure.initialRate", "1000")
    //每个进程每秒最多从kafka读取的数据条数
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "200")


    //读取kafka配置信息
    //创建一个kafkaParams
    var kafkaParams = Map[String, String]("bootstrap.servers" -> "172.20.1.104:9092,172.20.1.105:9092")

    if (firstReadLastest) {
      kafkaParams += ("auto.offset.reset" -> OffsetRequest.LargestTimeString)
    }

    //创建zkClient注意最后一个参数最好是ZKStringSerializer类型,不然写进去zk里面的偏移量是乱码
    val zkClient = new ZkClient("172.20.1.104:2181,172.20.1.105:2181,172.20.1.106:2181", 30000, 30000, ZKStringSerializer)
    //zk的路径
    val zkOffsetPath = "/sparkstreaming/20171219"
    //topic名字
    val topicSet = "sparkStreaming".split(",").toSet

    //创建StreamingContext,每隔多少秒一个批次t
    val scc = new StreamingContext(sparkConf, Seconds(10))

    //从流中创建Rdd
    val rdds: InputDStream[(String, String)] = createKafkaStream(scc, kafkaParams, zkClient, zkOffsetPath, topicSet)

    //DStream上的转换操作

    // 输入前10条到控制台，方便调试
    //处理逻辑
    //正确的应该是在worker中初始化连接
    rdds.foreachRDD(rdd => {
      //ps:如果在这里初始化的数据, // executed at the driver
      /*我们知道在集群模式下，上述代码中的connection需要通过序列化对象的形式从driver发送到worker，
      但是connection是无法在机器之间传递的，即connection是无法序列化的，
      这样可能会引起_serialization errors (connection object not serializable)_的错误。
      */
      //只处理有数据的rdd,没有数据的直接跳过
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(
          partitions => {
            //每个partition是内的rdd是运行在同一worker之上的
            //迭代分区,里面的代码是运行在executor上面
            //如果没有使用广播变量,连接资源就在这个地方初始化
            //比如数据库,hbase,redis // executed at the worker
            //遍历每个分区的数据
            val conn = MysqlManager.getMysqlManager.getConnection
            val statement = conn.createStatement
            try {
              //我们在提交Mysql的操作的时候，并不是每条记录提交一次，
              //而是采用了批量提交的形式，所以需要将conn.setAutoCommit(false)，这样可以进一步提高mysql的效率。
              conn.setAutoCommit(false)

              val ss = partitions.map( n => n._2.split("/"))
                .map(x => new Tuple3[String,String,String](x(0),x(1),x(2)))
              ss.foreach(msg => {
                log.info("--"+msg._1+"--"+msg._2+"--"+msg._3)
                //处理数据,处理逻辑
                val sql = "insert into test_vin(id, name, age) values ("+msg._1+",'"+msg._2+"',"+msg._3+")"
                log.info(sql.toString)
                statement.addBatch(sql)
              })
              statement.executeBatch
              conn.commit
            } catch {
              case e:Exception =>
                log.info("异常:" + e.toString)
            }finally {
              statement.close()
              conn.close()
            }

          })

        //更新每个批次的偏移量到zk中，注意这段代码是在driver上执行的
        KafkaOffsetManager.saveOffsets(zkClient, zkOffsetPath, rdd)
      }
    })



    //返回StreamContext
    scc
  }


  /**
    *
    * @param ssc          StreamingContext
    * @param kafkaParams  配置kafka的参数
    * @param zkClient     zk连接的client
    * @param zkOffsetPath zk里面偏移量的路径
    * @param topic        需要处理的topic
    * @return InputDStream[(String, String)] 返回输入流
    */
  def createKafkaStream(ssc: StreamingContext,
                        kafkaParams: Map[String, String],
                        zkClient: ZkClient,
                        zkOffsetPath: String,
                        topic: Set[String]): InputDStream[(String, String)] = {

    //目前仅支持一个topic的偏移量处理，读取zk里面偏移量字符串
    val zkOffsetData = KafkaOffsetManager.readOffsets(zkClient, zkOffsetPath, topic.last)

    val kafkaStream = zkOffsetData match {
      case None => //如果从zk里面没有读到偏移量,就说明是系统第一次启动
        log.info("系统第一次启动，没有读取到偏移量，默认就最新的offset开始消费")
        //使用最新的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)
      case Some(lastStopOffset) =>
        log.info("从zk中读取到偏移量,从上次的偏移量开始消费数据......" + Some(lastStopOffset))
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message())
        //使用上次停止时候的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, lastStopOffset, messageHandler)

    }
    //返回创建的kafkaStream
    kafkaStream


  }


  /**
    * 保存数据到数据库
    *
    * @param conn 数据库连接
    * @param sql  prepared statement sql
    * @param data 要保存的数据，Tuple3结构
    */
  def insert(conn: Connection, sql: String, data: (String, String, String)): Unit = {
    try {
      val ps = conn.prepareStatement(sql)
      ps.setString(1, data._1)
      ps.setString(2, data._2)
      ps.setString(3, data._3)
      ps.executeUpdate()
      ps.close()
    } catch {
      case e: Exception =>
        log.error("Error in execution of insert. " + e.getMessage)
    }
  }



}
