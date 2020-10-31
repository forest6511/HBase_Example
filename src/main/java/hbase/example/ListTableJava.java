package hbase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.stream.Stream;

public class ListTableJava {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        Stream.of(admin.listTableNames()).forEach(tab -> System.out.println(tab.getNameAsString()));

    }
}
