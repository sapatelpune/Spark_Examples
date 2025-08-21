package com.general;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class UnionTestSPR {

    public static void main(String[] args) {
       SparkSession session= SparkSession.builder().master("local[2]").getOrCreate();
        session.sparkContext().setLogLevel("Error");
        Dataset<Row> addressDS= session.read().format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep","|")
                .load("src/main/resources/uTest/uTest_add.csv");
        System.out.println("addressDS");
        addressDS.show(10);

        Dataset<Row> amtDS= session.read().format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep","|")
                .load("src/main/resources/uTest/uTest_amt.csv");
        System.out.println("amtDS");
        amtDS.show(10);

        Dataset<Row> gradeDS= session.read().format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep","|")
                .load("src/main/resources/uTest/uTest_grade.csv");

        System.out.println("gradeDS");
        gradeDS.show(10);
        // create unique dataset
        Dataset<Row> uniqueDS=(addressDS.unionByName(amtDS)
                .unionByName(gradeDS))
                .select("id","name")
                .sort("id")
                .distinct();
        System.out.println("uniqueDS");
        uniqueDS.show(10);

      // First Join
        Dataset<Row> uniqueWithAmt=uniqueDS.join(amtDS,
                uniqueDS.col("id").equalTo(amtDS.col("id"))
                ,"left")
                .select(uniqueDS.col("id")
                        ,uniqueDS.col("name"),
                        col("amount"));
        System.out.println("uniqueWithAmt");
        uniqueWithAmt.show(10);

     // Second Join
        Dataset<Row> uniqueWithAmtAndGrade=uniqueWithAmt.join(gradeDS,
                        uniqueWithAmt.col("id").equalTo(gradeDS.col("id"))
                        ,"left")
                .select(uniqueWithAmt.col("id")
                        ,uniqueWithAmt.col("name"),
                        uniqueWithAmt.col("amount"),
                        col("grade"));
        System.out.println("uniqueWithAmtAndGrade");
        uniqueWithAmtAndGrade.show(10);

     // Third Join
     Dataset<Row> uniqueWithAmtGradeAddress=uniqueWithAmtAndGrade.join(addressDS,
                     uniqueWithAmtAndGrade.col("id").equalTo(addressDS.col("id"))
                     ,"left")
             .select(uniqueWithAmtAndGrade.col("id")
                     ,uniqueWithAmtAndGrade.col("name"),
                     uniqueWithAmtAndGrade.col("amount"),
                     uniqueWithAmtAndGrade.col("grade"),
                     col("address"));
     System.out.println("uniqueWithAmtGradeAddress");
     uniqueWithAmtGradeAddress.show(10);
    }





}
