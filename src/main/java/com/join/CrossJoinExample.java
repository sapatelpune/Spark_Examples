package com.join;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CrossJoinExample {

    public static void main(String[] args) {
        SparkSession session= SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        //Loading Emp Data
        Dataset<Row> emp= session.read().format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep","|")
                .load("src/main/resources/empdata/empDummy.csv");

        // Loading Sal Data
        Dataset<Row> sal= session.read().format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep","|")
                .load("src/main/resources/empdata/salaryDummy.csv");

        System.out.println("======Emp Sal Cross Join=======");
        Dataset<Row> crossJoin =emp.crossJoin(sal);
        crossJoin.show(1000);

    }
}
