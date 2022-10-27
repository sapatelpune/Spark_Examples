package com.parquet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ConvertingCsvToParquet {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().master("local[4]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        //System.setProperty("hadoop.home.dir","C:\\hadoop" );
        Dataset<Row> df = session
                .read()
                .format("csv")
                .option("header", true)
                .option("inferschema",true)
                .option("sep",",")
                .load("src/main/resources/annual-enterprise-survey-2021-financial-year-provisional-csv.csv");
        df.write().mode("overwrite").parquet("src/main/resources/parquetData");
        System.out.println("========>"+df.count());
        df.show();

    }
}
