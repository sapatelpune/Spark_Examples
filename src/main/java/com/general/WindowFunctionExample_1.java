package com.general;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class WindowFunctionExample_1 {

    public static void main(String[] args) {
       SparkSession session= SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        Dataset<Row> dupData= session.read().format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep","|")
                .load("src/main/resources/sampleDataDupRecords.csv");
        // Create WindowSpec object
        WindowSpec windowSpec= Window. partitionBy("id");
        dupData.orderBy("id").show(10);
        dupData=dupData.groupBy("id").agg(collect_list("amount")).alias("ListAmt");
        dupData.show(10);

    }

}
