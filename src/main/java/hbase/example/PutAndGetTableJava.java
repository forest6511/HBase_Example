package hbase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

public class PutAndGetTableJava {

    final static String rowKey = "row";
    final static String colFam1 = "colfam1";
    final static String colFam2 = "colfam2";
    final static String colFam1Age = "age";
    final static String colFam1Name = "name";
    final static String colFam2Name = "gender";

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("sample-table");

        createTable(admin, tableName);

        Table table = connection.getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey + "-1"));
        put.addColumn(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Age), Bytes.toBytes("32"));
        put.addColumn(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Name), Bytes.toBytes("Mark"));
        put.addColumn(Bytes.toBytes(colFam2), Bytes.toBytes(colFam2Name), Bytes.toBytes("M"));
        table.put(put);

        put = new Put(Bytes.toBytes(rowKey + "-2"));
        put.addColumn(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Age), Bytes.toBytes("40"));
        put.addColumn(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Name), Bytes.toBytes("Hanna"));
        put.addColumn(Bytes.toBytes(colFam2), Bytes.toBytes(colFam2Name), Bytes.toBytes("F"));
        table.put(put);

        List<Get> gets = new ArrayList<Get>();

        Get get1 = new Get(Bytes.toBytes(rowKey + "-2"));
        get1.addColumn(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Age));
        get1.addColumn(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Name));
        get1.addColumn(Bytes.toBytes(colFam2), Bytes.toBytes(colFam2Name));
        gets.add(get1);

        Result[] results = table.get(gets);

        for (Result result : results) {
            printResults(result);
        }

    }

    public static void createTable(Admin admin, TableName tableName) throws IOException {
        TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFam1)).build())
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFam2)).build())
                .build();
        if (!admin.tableExists(tableName)) {
            admin.createTable(htd);
        }
    }

    public static void printResults(Result result) {

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = result.getMap();

        for (byte[] columnFamily : resultMap.keySet()) {
            String cf = Bytes.toString(columnFamily);
            NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = resultMap.get(columnFamily);

            for (byte[] column : columnMap.keySet()) {
                String col = Bytes.toString(column);
                NavigableMap<Long, byte[]> timestampMap = columnMap.get(column);

                for (Long timestamp : timestampMap.keySet()) {
                    String value = Bytes.toString(timestampMap.get(timestamp));
                    System.out.println("Column Family: " + cf
                            + " Column: " + col + " Value: " + value);
                }
            }
        }
    }
}
