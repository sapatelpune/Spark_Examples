package com.cache;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.functions.col;

public class CacheExample {

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
        // repartition the data into 10 partition
        df=df.repartition(10);
        df.cache();

        //df.count();  // if we use df.count spark will cache all 10 partition in memory (bea // due to df.count spark will cache all 10 partition in memorycuse it requires all the partions to calculate the count)

        df.take(10); // due to df.take(10) spark will cache only 1 partition  in memory (beacuse we need only 10 records i.e only 1 partition)
        Scanner scanner= new Scanner(System.in);
        scanner.nextLine();
    }
}
