package hbase.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

object CreateTableScala {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = HBaseConfiguration.create

    val connection: Connection = ConnectionFactory.createConnection(conf)

    val admin: Admin = connection.getAdmin
    val tableName: TableName = TableName.valueOf("notifications")

    val htd: TableDescriptor = TableDescriptorBuilder
      .newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("attributes".getBytes).build)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("metrics".getBytes).build)
      .build()

    if (!admin.tableExists(tableName)) {
      println("Creating table... ")
      admin.createTable(htd)
    }
    else {
      println("Underlying tables... ")
      admin.listTableNames.foreach(tab => println(tab.getNameAsString))
    }
  }
}
