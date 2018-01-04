package com

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/12/8.
  * //致命的缺点,可以从sparkUI看到,每个sql都是从新加载数据,性能缓慢
  */
object PeopleDataStatistics {
  private val schemaString = "id,gender,height"

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage:PeopleDataStatistics2 filePath")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Spark Analysis:PeopleDataStatistics")
    val sc = new SparkContext(conf)
    val peopleDateRdd = sc.textFile(args(0).trim);
    val sqlCtx = new SQLContext(sc)

    val schemaArr = schemaString.split(",")
    val schema = StructType(schemaArr.map(fieldName => StructField(fieldName,StringType,true)))
    val rowRdd : RDD[Row] = peopleDateRdd
      .map(_.split(","))
      .map(eachRow => Row(eachRow(0),eachRow(1),eachRow(2)))
    val peopleDF = sqlCtx.createDataFrame(rowRdd,schema)
    peopleDF.persist(StorageLevel.MEMORY_ONLY_SER)
    peopleDF.registerTempTable("people")


    //get the male people whose height is more than 180
    val higherMale180 = sqlCtx.sql("" +
      "select *" +
      " from people" +
      " where height > 180 and gender = 'F'")
    println("Men whose height are more than 180: " + higherMale180.count())  //job 0-stage0  ..这里才触发job的产生
    println("<Display #1>")

    //get the female people whose height is more than 170
    val higherMale170 = sqlCtx.sql("" +
      "select *" +
      " from people" +
      " where height > 170 and gender = 'M'")
    println("female whose height are more than 170: " + higherMale170.count())//job 1
    println("<Display #2>")

    //Grouped the people by gender and count the number
    peopleDF.groupBy(peopleDF("gender")).count().show()
    println("People Count Grouped By Gender")
    println("<Display #3>")

    //count and print the first 50 men with a height greater than 210cm
    peopleDF.filter(peopleDF("gender").equalTo("M")).filter(peopleDF("height") > 210).show(50)
    println("Men whose height is more than 210")
    println("<Display #4>")

    //To sort all people by height and print the top 50
    peopleDF.sort(peopleDF("height").desc).take(50).foreach { println }
    println("Sorted the people by height in descend order,Show top 50 people")
    println("<Display #5>")


    //The average height of a man was counted
    peopleDF.filter(peopleDF("gender").equalTo("M")).agg(Map("height" -> "avg")).show()
    println("The Average height for Men")
    println("<Display #6>")

    //The Max height of a women was counted
    peopleDF.filter(peopleDF("gender").equalTo("F")).agg("height" -> "max").show()
    println("The Max height for Women:")
    println("<Display #7>")
    //......
    println("All the statistics actions are finished on structured People data.")
    peopleDF.unpersist()
  }

}
