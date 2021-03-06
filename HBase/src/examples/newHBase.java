package examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class newHBase {

	public static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
	}

	public static void createTable(String tableName, String[] familys)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
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

	public static void addData(String tableName, String rowKey, String family, String qualifier, String value) {
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			System.out.println("data insert success!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		String tablename = "hbasetest";
		String[] familys = { "family" };
		try {
			createTable(tablename, familys);
			addData(tablename, "actor1", "family", "name", "liangchaowei");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
