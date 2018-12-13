package cn.com.benny.learn

import java.util.Properties

/**
  * <p>Description:<p>
  * QQ: 178542285
  *
  * @Author benny
  * @Date: 2018/12/13
  * @Time: 22:58
  */
object MysqlCfg {
    var properties = new Properties()
    var connUrl = "jdbc:mysql://192.168.1.111:3306/test"
    properties.setProperty("user", "root")
    properties.setProperty("password", "root123")
    properties.setProperty("useSSL", "true")
    properties.setProperty("driver", "com.mysql.jdbc.Driver")

}
