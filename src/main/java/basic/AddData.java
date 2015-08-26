package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.UUID;

public class AddData {

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.30.140.203");
        conf.set("hbase.master", "172.30.140.203");

        TableName tableName = creatTable(conf);
        addData(conf, tableName);
    }

    private static void addData(Configuration conf, TableName tableName) throws IOException {

        String rowKey = UUID.randomUUID().toString();

        Table table = ConnectionFactory
                .createConnection(conf)
                .getTable(tableName);


        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("Ionut Ando"));
        put.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes(""), Bytes.toBytes("ando@ando.ro"));
        table.put(put);
        System.out.println("insert recored " + rowKey + " to table "
                + tableName + " ok.");
    }

    public static TableName creatTable(Configuration conf)
            throws Exception {
        Admin admin = ConnectionFactory.createConnection(conf).getAdmin();
        TableName tableName = TableName.valueOf("ando-test");


        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("personal"));
            tableDescriptor.addFamily(new HColumnDescriptor("contactinfo"));
            tableDescriptor.addFamily(new HColumnDescriptor("creditcard"));
            admin.createTable(tableDescriptor);

            System.out.println("create table " + tableName + " ok.");
        }

        admin.close();

        return tableName;
    }
}
