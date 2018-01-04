package streaming
import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource
/**
  * Created by Administrator on 2017/12/19.
  */
class  MysqlPool extends Serializable{
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  try {
    cpds.setJdbcUrl("jdbc:mysql://192.168.200.42:3306/test?useUnicode=true&amp;characterEncoding=UTF-8");
    cpds.setDriverClass("com.mysql.jdbc.Driver");
    cpds.setUser("root");
    cpds.setPassword("123456")
    cpds.setMaxPoolSize(200)
    cpds.setMinPoolSize(20)
    cpds.setAcquireIncrement(5)
    cpds.setMaxStatements(180)
  } catch {
    case e: Exception => e.printStackTrace()
  }
  def getConnection: Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        null
    }
  }
}
object MysqlManager {
  var mysqlManager: MysqlPool = _
  def getMysqlManager: MysqlPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }
}
