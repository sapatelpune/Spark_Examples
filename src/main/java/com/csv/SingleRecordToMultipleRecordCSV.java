package com.csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class SingleRecordToMultipleRecordCSV {
	
	public static void main(String[] args) {
		SparkSession session = SparkSession
									.builder()
									.appName("multidelimiter csv Read")
									.master("local[2]")
									.getOrCreate();
		session.sparkContext().setLogLevel("Error");
		Dataset<Row> df = session
							.read()
							.format("text")
							.load("src/main/resources/SingleRecordToMultipleRecordCSV.csv");
		df.show(10,false);
		//df=df.withColumn("updated",regexp_replace)
		df=df.withColumn("updated",regexp_replace(col("value"),lit("Sandeep"),lit("Pradeep")));

		System.out.println("------------");
		df.show(10,false);
		System.out.println("------------");
		//df.printSchema();
		

		
	}

}
