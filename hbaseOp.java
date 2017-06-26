package hbase_operations;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class hbaseOp {

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		// TODO Auto-generated method stub

		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		System.out.println("Creating HTableDescriptor object...");
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("customer"));
		
		System.out.println("Adding column families to the HBase table...");
		
		HColumnDescriptor personal = new HColumnDescriptor("personal");
		System.out.println("Default max versions of personal column family: " + personal.getMaxVersions());
		System.out.println("Setting max versions of personal column family to 3");
		personal.setMaxVersions(3);
		System.out.println("Updated max versions of personal column family: " + personal.getMaxVersions());
		
		tableDescriptor.addFamily(personal);
		tableDescriptor.addFamily(new HColumnDescriptor("id"));
		tableDescriptor.addFamily(new HColumnDescriptor("name"));
		tableDescriptor.addFamily(new HColumnDescriptor("location"));
		tableDescriptor.addFamily(new HColumnDescriptor("age"));
		

		System.out.println("Disabling and Dropping table if already exists...");
		if (admin.isTableAvailable("customer")) {
			if (admin.isTableEnabled("customer")) {
				admin.disableTable("customer");
			}
			admin.deleteTable("customer");
		}
		
		System.out.println("Creating HBase Table...");
		admin.createTable(tableDescriptor);
		System.out.println("HBase Table Created!");
	}

}
