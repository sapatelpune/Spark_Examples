package com.general;

import org.apache.avro.generic.GenericData;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.lit;

public class MergeTwoDataFrames {

    public static void main(String[] args) {
        SparkSession session= SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("error");
        Dataset<Row> df1= session.read().
                format("csv").
                option("header",true).
                option("inferschema",true).
                option("sep","|").
                load("src/main/resources/SampleData1.csv");

        Dataset<Row> df2= session.read().
                format("csv").
                option("header",true).
                option("inferschema",true).
                option("sep","|").
                load("src/main/resources/SampleData2.csv");

       System.out.print("Raw Data1=== ");
        df1.show();
        System.out.print("Raw Data2=== ");
        df2.show();
        // Automated Approach
        // Get the array of columns from each dataframe
        String[] colsFirstDF=df1.columns();
        String[] colsSecondDF=df2.columns();

        // Convert Array to List Of String
       List<String> firstDFColLIst= Arrays.asList(colsFirstDF);
       List<String> secondDFColLIst= Arrays.asList(colsSecondDF);
        System.out.print("firstDFColLIst=== "+firstDFColLIst);
        System.out.print("secondDFColLIst=== "+secondDFColLIst);

        // Get the list of columns which are not availabe in List2 (List - List2)
       List <String> list1= (List<String>) CollectionUtils.subtract(firstDFColLIst, secondDFColLIst);
        // Get the list of columns which are not availabe in List1 (List2 - List1)
       List <String> list2= (List<String>) CollectionUtils.subtract(secondDFColLIst,firstDFColLIst);

        System.out.println("Columns Not Availabe in DF1="+list1);
        System.out.println("Columns Not Availabe in DF2="+list2);

        // Add the missing columns in DF1 witll null value from l2
        for(int i=0 ;i<list2.size();i++){
            df1=df1.withColumn(list2.get(i),lit(null));
        }

        // Add the missing column in in DF2
        for(int i=0 ;i<list1.size();i++){
            df2=df2.withColumn(list1.get(i),lit(null));
        }
        System.out.println("Updated Schema DF1=== ");
        df1.show();
        System.out.println("Updated Schema DF2=== ");
        df2.show();
        Dataset<Row> df3= df1.unionByName(df2);
        System.out.println("After Union=== ");
        df3.show();
    }
}
