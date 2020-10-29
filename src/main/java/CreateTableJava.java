import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.stream.Stream;

public class CreateTableJava {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("notifications");
        TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("attributes")).build())
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("metrics")).build())
                .build();
        if (!admin.tableExists(tableName)) {
            System.out.println("Creating table. ");
            admin.createTable(htd);
        } else {
            System.out.println("Underlying tables... ");
            Stream.of(admin.listTableNames()).forEach(tab -> System.out.println(tab.getNameAsString()));
        }
    }
}
