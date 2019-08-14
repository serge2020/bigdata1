package com.accenture.bootcamp.day1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Loader {

  protected def fromResource(resource: String): String = {
    new java.io.File("src/test/resources/" + resource).getCanonicalPath
  }

  def loadNewYearHonours(sc: SparkContext): RDD[String] = {
    // TODO Task #1: Create RDD from file `1918NewYearHonours.txt`
    val filePath= fromResource("1918NewYearHonours.txt")


    //val honours1918Rdd: RDD[String] = sc.textFile("/Users/sergejs/Documents/Sarakste/Study/BCamp/W2/OneDrive_2019-08-12/acn-bigdata-scala-intro/src/test/resources/1918NewYearHonours.txt")
    sc.textFile(filePath)

  }

  def loadAustralianTreaties(sc: SparkContext): RDD[String] = {
    // TODO Task #2: Create RDD from file `ListOfAustralianTreaties.txt`
    val filePath = fromResource("ListOfAustralianTreaties.txt")

    //val australianTreatiesRdd: RDD[String] = sc.textFile("/Users/sergejs/Documents/Sarakste/Study/BCamp/W2/OneDrive_2019-08-12/acn-bigdata-scala-intro/src/test/resources/ListOfAustralianTreaties.txt")
    sc.textFile(filePath)
  }


}
