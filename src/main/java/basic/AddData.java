package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class AddData {

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.30.140.203");
        conf.set("hbase.master", "172.30.140.203");

        creatTable(conf);
    }

    public static void creatTable(Configuration conf)
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
    }
}
