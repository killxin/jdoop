package examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class superHBase {

	public static Configuration conf = null;
	public static Connection connection = null;
	static {
		conf = HBaseConfiguration.create();
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void createTable(TableName tableName, String[] familys)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        Admin admin = connection.getAdmin();
		
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("table already exists!");
		}
		System.out.println("table create start!");
		HTableDescriptor tableDec = new HTableDescriptor(tableName);
		for (String str : familys) {
			tableDec.addFamily(new HColumnDescriptor(str));
		}
		admin.createTable(tableDec);
		System.out.println("table create success!");

	}

	public static void addData(TableName tableName, String rowKey, String family, String qualifier, String value) {
		try {
			Table table = connection.getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			System.out.println("data insert success!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		TableName tablename = TableName.valueOf("hbasetest");
		String[] familys = { "family" };
		try {
			createTable(tablename, familys);
			addData(tablename, "actor1", "family", "name", "liangchaowei");
			addData(tablename, "actor2", "family", "name", "zhangmanyu");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}