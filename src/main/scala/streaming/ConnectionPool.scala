package streaming
import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
/**
  * Created by Administrator on 2017/12/19.
  */
object ConnectionPool {
  val log = org.apache.log4j.LogManager.getLogger("ConnectionPool")
  //连接池配置
  private val connectionPool: Option[BoneCP] = {
    try{
      Class.forName("org.postgresql.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl("jdbc:postgresql://192.168.200.42/test")
      config.setUsername("root")
      config.setPassword("123456")
      config.setLazyInit(true)

      config.setMinConnectionsPerPartition(3)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)
      Some(new BoneCP(config))
    }catch {
      case exception: Exception =>
        log.warn("Create Connection Error: \n" + exception.printStackTrace())
        None
    }
  }

  // 获取数据库连接
  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(pool) => Some(pool.getConnection)
      case None => None
    }
  }

  // 释放数据库连接
  def closeConnection(connection:Connection): Unit = {
    if(!connection.isClosed) {
      connection.close()
    }
  }
}
