package com.general;

import org.apache.spark.sql.SparkSession;

public class CreateMultipleSparkSessionsExample {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();

        // This line will return already created session
        //SparkSession sessionNew = SparkSession.builder().master("local[2]").getOrCreate();

        // This will create new spark session
        SparkSession sessionNew = session.newSession();
        System.out.println(sessionNew);
        System.out.println(session);
    }
}
