package hbase.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._

object ListTableScala {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = HBaseConfiguration.create

    val connection = ConnectionFactory.createConnection(conf)
    connection.getAdmin.listTableNames.foreach(tab => println(tab.getNameAsString))
  }
}
