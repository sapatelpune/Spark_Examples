package com.general;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class WindowFunctionExample {

    public static void main(String[] args) {
       SparkSession session= SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        Dataset<Row> dupData= session.read().format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep","|")
                .load("src/main/resources/sampleDataDupRecords.csv");
        // Create WindowSpec object
        WindowSpec windowSpec= Window.
                                     partitionBy("id")
                                    .orderBy(desc("amount"));

        dupData=dupData.withColumn("row_number",row_number().over(windowSpec));

        System.out.println("Showing with the Row Number/ Rank==");
        dupData.orderBy(col("id"),col("row_number")).show();

        // Filter the records for Row Id 1
        Dataset<Row> updatedData=dupData.filter(dupData.col("row_number").equalTo(1));
        System.out.println("Showing with the Row Number/ Rank after removing duplicate");
        updatedData.orderBy(col("id")).show();

    }

}
