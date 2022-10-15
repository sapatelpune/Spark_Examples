package com.join;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InnerJoinTest {

    public static void main(String[] args) {
        SparkSession session= SparkSession.builder().master("local[2]").getOrCreate();
        //Loading Emp Data
        Dataset<Row> emp= session.read().format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep","|")
                .load("src/main/resources/empdata/emp.csv");

        // Loading Sal Data
        Dataset<Row> sal= session.read().format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep","|")
                .load("src/main/resources/empdata/salary.csv");

        System.out.println("======Emp Sal Join=======");
        // InnerJoin by Default
        Dataset<Row> empSalDF=emp.join(sal,
                                emp.col("id").equalTo(sal.col("id")));
        System.out.println("======Showing witout Drop=======");
        empSalDF.show();
        System.out.println("======Showing after Drop=======");
        empSalDF=empSalDF.drop(sal.col("id"));
        empSalDF.show();

    }
}
