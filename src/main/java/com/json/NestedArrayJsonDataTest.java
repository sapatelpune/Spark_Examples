package com.json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

public class NestedArrayJsonDataTest {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        Dataset<Row> df = session.read()
                .format("json")
                .option("multiline", true)
                .load("src/main/resources/json-data/NestedArray.json");

        df.printSchema();
        // Showing without dropping any columns
        df.show();
        df=df.withColumn("publisherName", col("publisher.name"));
        df=df.withColumn("authorName", col("author.name"));
        df=df.withColumn("authorName", col("author.name"));
        df=df.withColumn("book",explode(col("books")));
        df=df.drop("author","publisher","books");
        df.printSchema();
        df.show(false);
        df=df.withColumn("bookName",col("book.title"));
        //df=df.withColumn("SalesByMonth",explode(col("book.salesByMonth")));
        df=df.withColumn("janSales",col("book.salesByMonth").getItem(0));
        df=df.withColumn("febSales",col("book.salesByMonth").getItem(1));

        System.out.println("Print Schema after last explode ======");
        df.printSchema();
        df.show(false);
    }
}
