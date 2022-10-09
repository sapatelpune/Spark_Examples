package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class ReadCSVFile {
	
	public static void main(String[] args) {
		SparkSession session = SparkSession
									.builder()
									.appName("TestPractice")
									.master("local[2]")
									.getOrCreate();
		
		Dataset<Row> df = session
							.read()
							.format("csv")
							.option("header", true)
							.option("inferschema",true)
							.option("sep",",")
							.load("src/main/resources/10000_Sales_Records.csv");
		
		df.show(10);
		df.printSchema();
		
		// Filter Dataframe based on multiple conditions
		System.out.println("-- Showing Filter Example --------------");
		Dataset<Row> dfFilterEx1=df
							.filter(df.col("Region").equalTo("Asia"))
							.filter(df.col("Sales Channel").equalTo("Online"));
		dfFilterEx1.show(10);
		
		System.out.println("-- Showing Group By Example --------------");
		
		System.out.println("Press Enter...To See group by ");
		Scanner scanner= new Scanner(System.in);
		scanner.nextLine();
		
		Dataset<Row> dfGroupBy=df.groupBy(df.col("Region"),df.col("Sales Channel")).count();
		dfGroupBy.show(10);
		
		System.out.println("Press Enter...To eNDs ");
		scanner= new Scanner(System.in);
		scanner.nextLine();
		
		
		/*System.out.println("-- Showing filter with Group by --------------");
		Dataset<Row> df2Ex= df.filter(df.col("Item Type").equalTo("Beverages"))
								.groupBy(df.col("Country")).count();
		
		df2Ex.show(10);
		
		System.out.println("-- Showing UDF --------------");
		//Example of UDF
		session.udf().register("CalculatePercentage", (Double profit,Double cost)-> profit*100/cost, DataTypes.DoubleType);
		session.udf().register("getColorCode", (Double percentage)-> percentage>=50 ? "Green" : percentage<50 && percentage>=40 ? "Yellow" : "Red" , DataTypes.StringType);
		Dataset<Row> dfUDFEx1=df.withColumn("Percentage", callUDF("CalculatePercentage",df.col("Total Profit"),df.col("Total Cost")));
		dfUDFEx1=dfUDFEx1.withColumn("Color Code", callUDF("getColorCode",df.col("Percentage")));
		dfUDFEx1.show(10);
	*/
		
		
	}

}
