package com.csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.FailFastMode;

public class HandleInvalidRecords {

    public static void main(String[] args) {
        SparkSession session= SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("error");
        Dataset<Row> df= session.read().
                        format("csv").
                        option("header",true).
                        option("inferschema",true).
                        option("sep","|").
                        option("Mode", "FAILFAST"). // Will throw  Caused by
                                                    // java.lang.RuntimeException: Malformed CSV record
                        //option("Mode", "DROPMALFORMED").
                        //option("Mode","PERMISSIVE"). // Default option will allow all the records
                        load("src/main/resources/SampleDataInvalidRecords.csv");

        df.printSchema();
        df.show();
    }
}
