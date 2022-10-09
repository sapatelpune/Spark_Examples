package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

/**
 * Input :
 *
 * 	Name|address
 * 	Sandeep|Pune
 * 	Saurabh|Mumbai
 * 	Nitu|Delhi
 * 	Nikhr|Pune
 * 	Maya|Indore
 *
 * Output (Replace Pune to Nagpur)
 *
 * Name|address
 * Sandeep|Nagpur
 * Saurabh|Mumbai
 * Nitu|Delhi
 * Nikhr|Nagour
 * Maya|Indore
 *
 */
public class ReplaceValueInColumn {
	
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
							.option("sep","|")
							.load("src/main/resources/SampleData.csv");
		
		df.show(10);
		df.printSchema();

		// Replace address form Pune to Nagpur
		df=df.withColumn("city_updated",
							regexp_replace(col("address"),
									lit("Pune"),
									lit("Nagpur")));

		System.out.println("-----Updated-------");
		df.show();
		System.out.println("------------");

	}

}
