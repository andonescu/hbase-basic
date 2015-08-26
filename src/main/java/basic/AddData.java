package basic;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class AddData {

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.30.140.203");
        conf.set("hbase.master", "172.30.140.203");

        TableName tableName = creatTable(conf);
        addData(conf, tableName,
                Lists.newArrayList(
                        new Phonebook("Ionut", "Ando", "+4076666666"),
                        new Phonebook("Dan", "Rares", "+07662738733"),
                        new Phonebook("Mircea", "Popa", "+07662738733")
                )
        );

        getAllRecord(conf, tableName);

        deleteTableIfExists(conf, tableName);

    }

    public static void getAllRecord(Configuration conf, TableName tableName) {
        try {
            Table table = ConnectionFactory
                    .createConnection(conf)
                    .getTable(tableName);

            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for (Result r : ss) {
                for (KeyValue kv : r.raw()) {
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void deleteTableIfExists(Configuration conf, TableName tableName) throws IOException {
        Admin admin = ConnectionFactory.createConnection(conf).getAdmin();

        if (!admin.tableExists(tableName)) {
            System.out.println("table does not exists!");
        } else {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        admin.close();
    }

    private static void addData(Configuration conf, TableName tableName, List<Phonebook> phonebook) throws IOException {

        String rowKey = UUID.randomUUID().toString();

        Table table = ConnectionFactory
                .createConnection(conf)
                .getTable(tableName);

        for (Phonebook contact : phonebook) {

            Put put = new Put(
                    Bytes.toBytes(String.format("%s-%s-%s", contact.getFirstname(), contact.getLastname(), contact.getPhone()))
            );

            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes(""), Bytes.toBytes(""));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");

        }

        table.close();
    }

    public static TableName creatTable(Configuration conf)
            throws Exception {
        Admin admin = ConnectionFactory.createConnection(conf).getAdmin();
        TableName tableName = TableName.valueOf("phonebook-ando-op1");


        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("data"));


            admin.createTable(tableDescriptor);

            System.out.println("create table " + tableName + " ok.");
        }

        admin.close();

        return tableName;
    }
}

class Phonebook {
    private String firstname;
    private String lastname;
    private String phone;

    public Phonebook(String firstname, String lastname, String phone) {
        this.firstname = firstname;
        this.lastname = lastname;
        this.phone = phone;
    }

    @Override
    public String toString() {
        return "Phonebook{" +
                "firstname='" + firstname + '\'' +
                ", lastname='" + lastname + '\'' +
                ", phone='" + phone + '\'' +
                '}';
    }

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }
}
