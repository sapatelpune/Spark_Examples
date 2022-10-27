package com.general;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class AggregateFunctionTest {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().appName("TestPractice").master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("error");

        Dataset<Row> df = session.read().format("csv").option("header", true).option("inferschema",true).option("sep","|")
                .load("src/main/resources/TX_Data_Sample.csv");

        // Get the transaction amount group by City
        System.out.println("===Get the transaction amount group by City===");
        df.groupBy(col("tx_city"))
                .sum("tx_amount")
                .show();

        // transaction count by city
        System.out.println("=== Transaction count by city==");
        df.groupBy(col("tx_city"))
                .count()
                .show();

        // Find the maximum transaction amount
        System.out.println("=== Find the min transaction amount==");
        df.agg(min(col("tx_amount"))).show();
        //or
        df.select(min(col("tx_amount"))).show();

        System.out.println("=== Find the max transaction amount==");
        df.select(max(col("tx_amount"))).show();

        System.out.println("=== order by Tx Amount and agent_name ==");
        df.orderBy("tx_amount","agent_name").show();
        df.show();

        System.out.println("=== sumDistinct ==");
        df.select(sumDistinct("tx_amount")).show();

        // Get Max Transaction amount by City
        System.out.println("=== Get Max Transaction amount by City ==");
        df.groupBy("tx_city")
                .agg(max("tx_amount")).show();

        // Get Max Transaction amount by City
        System.out.println("=== Get Max Transaction amount and Sum of tx amount by City ==");
        df.groupBy("tx_city")
                .agg(max("tx_amount"),
                        min("tx_amount"),
                        sum("tx_amount")).show();

        System.out.println("=== collect all the transaction amount in list ==");
        Dataset<Row> df1= df.select(collect_list("tx_amount").as("amounts"));
        df1.printSchema();
        df1.show(false);


        System.out.println("=== collect all the transaction amount in list/array and unique cities ==");
        df1= df.select(collect_list("tx_amount").as("amounts"),
                        collect_set(col("tx_city")).as("cities"));

        df1.printSchema();
        df1.show(false);

    }


}
