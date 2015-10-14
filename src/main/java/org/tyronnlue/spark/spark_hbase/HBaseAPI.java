package org.tyronnlue.spark.spark_hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseAPI {
	static Configuration conf=null;
	static{
		conf=HBaseConfiguration.create();
		//conf.set("hbase.zookeeper.quorum", "localhost");
	}
	
	/**
	 * 创建表
	 * @param tableName
	 * @param family
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 */
	public static void createTable(String tableName,String[] family) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		//HBaseAdmin用于创建表
		HBaseAdmin admin=new HBaseAdmin(conf);
		//HTableDescriptor列族描述符，用于添加列祖
		HTableDescriptor desc=new HTableDescriptor(tableName);
		for(String fam:family){
			desc.addFamily(new HColumnDescriptor(fam));
		}
		
		if(admin.tableExists(tableName)){
			System.out.printf("table %s already exists\n",tableName);
			System.exit(0);
		}else{
			admin.createTable(desc);
			System.out.printf("table %s create success!!",tableName);
		}
		
		
	}

}
