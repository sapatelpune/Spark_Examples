package com.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlTempViewAndGlobalViewExample {

    public static void main(String[] args) {
        // Create 2 different spark sessions
        SparkSession session1 = SparkSession.builder().master("local[2]").getOrCreate();
        SparkSession session2 = session1.newSession();

        System.out.println(session1);
        System.out.println(session2);
        session1.sparkContext().setLogLevel("Error");
        session2.sparkContext().setLogLevel("Error");

        Dataset<Row> surveyDF = session1.read().format("csv").option("header", true).option("sep", ",").option("inderschema", true)
                .load("src/main/resources/annual-enterprise-survey-2021-financial-year-provisional-csv.csv");
        // Created TempView
        surveyDF.createOrReplaceTempView("survey_data");
        // Create Global Temp View
        surveyDF.createOrReplaceGlobalTempView("global_survey_data");

        System.out.println("====Session 1 ==> Data from  temp view=");
        Dataset<Row> df1=session1.sql("Select Year, sum(Value) from survey_data group by year order by year");
        df1.show(2);

        System.out.println("====Session 1 ==>Data from GlobalTempView=");
        Dataset<Row> df2=session1.sql("Select Year, sum(Value) from global_temp.global_survey_data group by year order by year");
        df2.show(2);

        //session1.close(); You won't be able to access the data if you close the session1
        System.out.println("====Session 2 ==> Data from  GlobalTempView=");
        Dataset<Row> df3=session2.sql("Select Year, sum(Value) from global_temp.global_survey_data group by year order by year");
        df3.show(2);

        System.out.println("====Data from survey_data in Session2=");
        Dataset<Row> df4=session2.sql("Select Year, sum(Value) from survey_data group by year order by year");
        df4.show(2);

    }
}