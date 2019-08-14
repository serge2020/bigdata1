package com.accenture.bootcamp

import com.accenture.bootcamp.day1.Tokenizer
import com.accenture.bootcamp.day1.Loader
import com.accenture.bootcamp.day1.Tasks
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.math._




/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {

    val conf:SparkConf = new SparkConf().setAppName("SparkJob").setMaster("local")
    val sc:SparkContext = new SparkContext(conf)

    val honours1918Rdd = Loader.loadNewYearHonours(sc)
    val australianTreatiesRdd = Loader.loadAustralianTreaties(sc)

    val tHonours1918 = Tokenizer.tokenize(honours1918Rdd)
    val tAustralianTreaties = Tokenizer.tokenize(australianTreatiesRdd)

    //val lines = sc.parallelize(List("1842 – Treaty 5 March treaty 1856)[5]"))

    val lines_alt = ("1842 – Treaty 5 March treaty 1856)[5]")

    val answer3 = Tokenizer.words("1842 – Treaty 5 March 1856) [5]")

    val answer4 = Tokenizer.countWords(tHonours1918)

    val answer5 = Tokenizer.countWords(tAustralianTreaties)

    val answer7 = Tokenizer.numbersAlt(answer3)

    val answer8 = Tokenizer.numbers(australianTreatiesRdd).distinct().count()

    val answer9 = ceil(Tokenizer.numbers(australianTreatiesRdd).sum()/Tokenizer.numbers(australianTreatiesRdd).count())

    val answer10_2 = Tokenizer.wordFrequencyAlt(Tokenizer.words(lines_alt))

    //val answer10 = Tokenizer.wordFrequency(lines.flatMap(Tokenizer.words)).map(_.toLowerCase).collect()

    val answer11 = Tokenizer.wordFrequency(australianTreatiesRdd)

    //val task5 = sc.parallelize(List("Welcome\tto\nAccenture\rLatvia! End"))


    println( "Hello World!" )
    //println("concat arguments = " + foo(args))

    //println(" There are " + Tokenizer.countWords(lines1) + " lines in 1918 Honours")


    println("Task 1: printing beginning of NYHonours RDD")
    honours1918Rdd.collect().take(5).foreach(println)
    println()

    println("Task 2: printing beginning of Australian Treaties RDD")
    australianTreatiesRdd.collect().take(5).foreach(println)
    println()

    println("Task 3:")
    answer3.foreach(println)
    println()

    //tHonours1918.collect().take(5).foreach(println)

    println("Answers to the tasks 4 - 6 :")
    println()
    println("Honours 1918 has " + answer4 + " words")
    println("Australian Treaties has " + answer5 + " words")
    println("Together they have " + (answer4 + answer5) + " words")
    println()

    println("Task 7, printing only numbers :")
    answer7.foreach(println)
    println()

    //answer8.collect().take(5).foreach(println)

    println("Task 8, count of unique numbers in Australian treaties:")
    //answer8.collect().take(5).foreach(println)
    println(answer8)
    println()

    println("Task 9, the average value of all numbers in Australian treaties:")
    println(answer9)
    println()

    println("Task 10, word occurrence and counts:")
    answer10_2.foreach(println)
    println()

    println("Task 11, the most frequent words in Australian treaties are:")
    answer11.collect().take(10).foreach(println)


  }

}
