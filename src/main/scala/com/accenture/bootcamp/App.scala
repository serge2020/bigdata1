package com.accenture.bootcamp

import com.accenture.bootcamp.day1.Tokenizer
import com.accenture.bootcamp.day1.Loader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


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

    val lines_alt = ("1842 – Treaty 5 March treaty 1856)[5]")
    val sampleWord = "."

    val answer3 = Tokenizer.words("1842 – Treaty 5 March 1856) [5]")

    val answer4 = Tokenizer.countWords(tHonours1918)

    val answer5 = Tokenizer.countWords(tAustralianTreaties)

    val answer7 = Tokenizer.numbersAlt(answer3)

    val answer8 = Tokenizer.numbers(australianTreatiesRdd).distinct().count()

    val answer9 = ceil(Tokenizer.numbers(australianTreatiesRdd).sum()/Tokenizer.numbers(australianTreatiesRdd).count())

    val answer10_2 = Tokenizer.wordFrequencyAlt(Tokenizer.words(lines_alt))

    val answer11 = Tokenizer.wordFrequency(australianTreatiesRdd)

    val answer12b = Tokenizer.wordClassifier(sampleWord)

    val answer13 = Tokenizer.classify(tAustralianTreaties)

    val answer14b = Tokenizer.classifySamples(Tokenizer.tokenize(Loader.loadAustralianTreaties(sc)))
      .sortByKey().distinct().collect().take(20)

    println( "Hello World!" )

    println("Task 1: printing beginning of NYHonours RDD")
    honours1918Rdd.collect().take(5).foreach(println)
    println()

    println("Task 2: printing beginning of Australian Treaties RDD")
    australianTreatiesRdd.collect().take(5).foreach(println)
    println()

    println("Task 3:")
    answer3.foreach(println)
    println()

    println("Answers to the tasks 4 - 6 :")
    println()
    println("Honours 1918 has " + answer4 + " words")
    println("Australian Treaties has " + answer5 + " words")
    println("Together they have " + (answer4 + answer5) + " words")
    println()

    println("Task 7, printing only numbers :")
    answer7.foreach(println)
    println()


    println("Task 8, count of unique numbers in Australian treaties:")
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
    println()

    println("Task 12, classification (A, B, C, D) of string 'mama':")
    println(Tokenizer.wordStats(sampleWord))
    println()
    println("This string belongs to the Group:")
    println(answer12b)
    println()

    println("Task 13, number of words in word groups:")
    answer13.foreach(println)
    println()

    println("Samples of each group from Australian treaties:")
    answer14b.foreach(println)


  }

}
