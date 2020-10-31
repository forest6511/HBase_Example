package hbase.example

import java.io.IOException
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}

object PutAndGetTableScala {

  val rowKey = "row-1"
  val colFam1 = "colfam1"
  val colFam2 = "colfam2"
  val colFam1Age = "age"
  val colFam1Name = "name"
  val colFam2Name = "gender"

  def main(args: Array[String]): Unit = {
    val conf: Configuration = HBaseConfiguration.create

    val connection: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = connection.getAdmin
    val tableName: TableName = TableName.valueOf("sample-table")

    createTable(admin, tableName)

    val table: Table = connection.getTable(tableName)
    val put: Put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Age), Bytes.toBytes("32"))
    put.addColumn(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Name), Bytes.toBytes("Mark"))
    put.addColumn(Bytes.toBytes(colFam2), Bytes.toBytes(colFam2Name), Bytes.toBytes("M"))
    table.put(put)

    val gets = new util.ArrayList[Get]

    val get1: Get = new Get(Bytes.toBytes(rowKey))
    get1.addColumn(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Age))
    get1.addColumn(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Name))
    get1.addColumn(Bytes.toBytes(colFam2), Bytes.toBytes(colFam2Name))
    gets.add(get1)

    val results = table.get(gets)

    for (result <- results) {
      printResults01(result)
    }
  }

  @throws[IOException]
  def createTable(admin: Admin, tableName: TableName): Unit = {
    val htd = TableDescriptorBuilder
      .newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFam1)).build)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFam2)).build).build
    if (!admin.tableExists(tableName)) admin.createTable(htd)
  }

  def printResults01(result: Result): Unit = {
    val it = result.listCells().iterator()
    val b = new StringBuilder

    b.append(Bytes.toString(result.getRow) + ":")
    while (it.hasNext) {
      val cell = it.next()
      val q = Bytes.toString(CellUtil.cloneQualifier(cell))
      val cf = Bytes.toString(CellUtil.cloneFamily(cell))
      b.append(s"($cf $q,${Bytes.toString(CellUtil.cloneValue(cell))})")
    }
    println(b)
    println(".....")
    printResults02(result)
  }

  def printResults02(result: Result): Unit = {
    val resultMap = result.getMap

    resultMap.keySet().forEach(columnFamily => {
      val cf = Bytes.toString(columnFamily)
      resultMap.get(columnFamily).keySet().forEach(column => {
        val col = Bytes.toString(column)
        val timestampMap = resultMap.get(columnFamily).get(column)
        timestampMap.keySet().forEach(timestamp => {
          val value = Bytes.toString(timestampMap.get(timestamp))
          println(s"Column Family:$cf Column: $col Value: $value")
        })
      })
    })
  }
}
