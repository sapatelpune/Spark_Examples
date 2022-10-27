package com.cache;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.Scanner;

import static org.apache.spark.sql.functions.col;

public class CheckPointExample {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[4]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        session.sparkContext().setCheckpointDir("C:\\checkpoint");
        //System.setProperty("hadoop.home.dir","C:\\hadoop" );
        Dataset<Row> df = session
                .read()
                .format("csv")
                .option("header", true)
                .option("inferschema",true)
                .option("sep",",")
                .load("src/main/resources/annual-enterprise-survey-2021-financial-year-provisional-csv.csv");
        // repartition the data into 10 partition
        System.out.println(df.count());
        df=df.filter(col("year").equalTo("2011"));
        df=df.checkpoint();

        /*  After filtering data , we are appling the checkpoint...
        Spake will forget the lineage (filter steps) and simpely add the data into checkpoint directory
        Spark  will not know how the data is transformed before the checkpoint applied
        But this is not the case with the cache or persist (Spark will keep the lineage and in case of any node failure it will recalculate the cache)
        */
        Scanner scanner= new Scanner(System.in);
        scanner.nextLine();
    }
}
