package com.general;

import org.apache.spark.sql.SparkSession;

public class CreateMultipleSparkSessionsExample {

    public static void main(String[] args) {
        SparkSession session1 = SparkSession.builder().master("local[2]").getOrCreate();

        // This line will return already created session i.e session1
        //SparkSession session2 = SparkSession.builder().master("local[2]").getOrCreate();

        // This will create new spark session
        SparkSession session2 = session1.newSession();
        System.out.println(session1);
        System.out.println(session2);
    }
}
