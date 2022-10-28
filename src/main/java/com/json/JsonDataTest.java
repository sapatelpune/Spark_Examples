package com.json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

public class JsonDataTest {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        Dataset<Row> df = session.read()
                .format("json")
                .option("multiline", true)
                .load("src/main/resources/json-data/shipment.json");

        df.printSchema();
        // Showing without dropping any columns
        df.show();

        df=df.withColumn("customerName",col("customer.name"));
        df=df.withColumn("customerCity",col("customer.city"));
        df=df.withColumn("supplierName",col("supplier.name"));
        df=df.withColumn("supplierCity",col("supplier.city"));

        // explode Books Array
        df=df.withColumn("item", explode(df.col("books")));
        System.out.println("Print Schema after explode ======");
        df.printSchema();

        // getting the qty and Book Name form the Array
        df=df.withColumn("Qty",col("item.qty"));
        df=df.withColumn("BookName",col("item.title"));

        // Dropping all the unused columns
        df=df.drop("customer","supplier","books","item");
        df.show(false);

    }
}
