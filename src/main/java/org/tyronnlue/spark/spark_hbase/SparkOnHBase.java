package org.tyronnlue.spark.spark_hbase;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkOnHBase {
	public static void main(String[] args){
		//SparkConf conf=new SparkConf().setAppName("SparkOnHBase");
		//JavaSparkContext sc=new JavaSparkContext(conf);
		SparkConf conf=new SparkConf().setAppName("Simple Application");
		conf.setMaster("local");
//		String sparkConf=System.getenv("SPARK_HOME");
//		System.out.println(sparkConf);
//		JavaSparkContext sc=new JavaSparkContext(master, "SparkOnHBase", System.getenv("SPARK_HOME"));
		String logFile="/Users/luhao/spark_practice/logFile";
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<String> logData=sc.textFile(logFile).cache();
		
		long numAs=logData.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return s.contains("a");
			}
		}).count();
		
		long numSs=logData.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception{
				return s.contains("s");
			}
		}).count();
		
		System.out.println("The lines which contains \"a\" are :"+numAs);
		System.out.println("The lines which contains \"s\" are :"+numSs);
		
		
	}

}
