package com.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlExample {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        Dataset<Row> surveyDF = session.read().format("csv").option("header", true).option("sep", ",").option("inderschema", true)
                .load("src/main/resources/annual-enterprise-survey-2021-financial-year-provisional-csv.csv");
        surveyDF.createOrReplaceTempView("survey_data");
        //Dataset<Row> df1=session.sql("Select * from survey_data");
        Dataset<Row> df1=session.sql("Select Year, sum(Value) from survey_data group by year order by year");
        df1.show(20);


    }
}