package com.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqJoinlExample {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        Dataset<Row> surveyDF = session.read()
                .format("csv")
                .option("header", true)
                .option("sep", ",")
                .option("inderschema", true)
                .load("src/main/resources/annual-enterprise-survey-2021-financial-year-provisional-csv.csv");
        surveyDF.createOrReplaceTempView("survey_data");

        Dataset<Row>  yearValueDF = session
                .read()
                .format("csv").option("header", true).option("inferschema",true)
                .option("sep","|").load("src/main/resources/broadcast_data.csv");

        yearValueDF.createOrReplaceTempView("year_value");

        Dataset<Row> joinDF=session.sql("Select sd.Year,sd.Value,yv.code from survey_data as sd," +
                                                        "year_value as yv " +
                                                        "where sd.year=yv.year");
        joinDF.show(5);

    }
}