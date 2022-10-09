package com.csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

/**
 * Input
 * Sandeep~;Pune
 * Saurabh~;Mumbai
 * Nitu~;Pune
 * Nikhr~;Pune
 *
 * OutPut
 * +-------+-------+
 * |Name   |address|
 * +-------+-------+
 * |Sandeep|Pune   |
 * |Saurabh|Mumbai |
 * |Nitu   |Pune   |
 * |Nikhr  |Pune   |
 * +-------+-------+
 */
public class ReadMultiDelimiterCSV {
	
	public static void main(String[] args) {
		// Created Spark Session
		SparkSession session = SparkSession
									.builder()
									.appName("multidelimiter csv Read")
									.master("local[2]")
									.getOrCreate();
		session.sparkContext().setLogLevel("Error");

		// Read data as text File
		Dataset<Row> df = session
							.read()
							.format("text")
							.load("src/main/resources/MultiDelimeterCsvSample.csv");


		System.out.println("====Showing the Raw Data");
		df.show(5, false);

		// Replace ~ (first delimeter) with blank )
		df=df.withColumn("updatedValue",regexp_replace(col("value"),
												lit("~"),
												lit("")));
		System.out.println("====Showing the Data after replaceing the first delimeter with blank");
		df.show();

		// Split the data based on the second delimeter into 2 columns
		//		Name;address
		//		Sandeep;Pune

		Dataset<Row> df2 = df.select(split(col("updatedValue"),";").getItem(0).as("Name"),
									 split(col("updatedValue"),";").getItem(1).as("address"));
		df2.printSchema();
		df2.show(10,false);

	}

}
