package com.general;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class UnionTest {

    public static void main(String[] args) {
       SparkSession session= SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        Dataset<Row> data1= session.read().format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep","|")
                .load("src/main/resources/uTest/uTest_10102022.csv");
        data1=data1.withColumn("fileName", lit(getFileName(data1.inputFiles()[0])));

        Dataset<Row> data2= session.read().format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep","|")
                .load("src/main/resources/uTest/uTest_20102022.csv");
        data2=data2.withColumn("fileName", lit(getFileName(data2.inputFiles()[0])));
        data1.show(false);
        data2.show(false);

        Dataset<Row> dataUnion=data1.union(data2);

        //row_number
        WindowSpec windowSpec= Window.partitionBy("id").orderBy(desc("fileName"));
        dataUnion=dataUnion.withColumn("row_number",row_number().over(windowSpec));
        dataUnion.show(false);

        dataUnion=dataUnion.filter(dataUnion.col("row_number").equalTo("1"));
        dataUnion.show();
    }

    public static String getFileName(String fullPath){

        System.out.println("fullPath="+fullPath);
        String[] tokens=fullPath.split("/");
        System.out.println(tokens.length);
        String fileName=tokens[tokens.length-1];
        System.out.println(fileName);
        // fileName=uTest_10102022.csv
        return fileName.substring(6,14);

    }

}
