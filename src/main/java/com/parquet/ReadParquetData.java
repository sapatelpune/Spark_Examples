package com.parquet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadParquetData {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        //System.setProperty("hadoop.home.dir","C:\\hadoop" );
        Dataset<Row> df = session
                .read()
                .format("parquet")
                .option("inferschema",true)
                .load("src/main/resources/parquetData/");

        System.out.println("========>"+df.count());
        df.show();

    }
}
