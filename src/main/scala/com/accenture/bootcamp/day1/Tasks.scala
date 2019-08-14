package com.accenture.bootcamp.day1

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait Tasks {

  def sc: SparkContext

  val newYearHonours: RDD[String] = Loader.loadNewYearHonours(sc)
  val australianTreaties: RDD[String] = Loader.loadAustralianTreaties(sc)

  /**
    * Task #5: How many words are in ListOfAustralianTreaties.txt?
    * Hint: use countWords() to count amount of words
    *
    * @return amount of words
    */
  def task5(): Long = {
    // TODO Task #5: How many words are in ListOfAustralianTreaties.txt?
    val australianTreatiesRdd = Loader.loadAustralianTreaties(sc)
    val tAustralianTreaties = Tokenizer.tokenize(australianTreatiesRdd)
    val answer5 = Tokenizer.countWords(tAustralianTreaties)
    answer5
  }

  /**
    * Task #6: How many words are in both .txt files?
    * Hint: use countWords() to count amount of words
    *
    * @return amount of words in both .txt files
    */
  def task6(): Long = {
    // TODO Task #6: How many words are in both .txt files?
    val honours1918Rdd = Loader.loadNewYearHonours(sc)
    val tHonours1918 = Tokenizer.tokenize(honours1918Rdd)
    val answer4 = Tokenizer.countWords(tHonours1918)
    val answer6 = answer4 + task5()
    answer6

  }

  /**
    * Task #8: How many unique numbers are in ListOfAustralianTreaties.txt? 
    *
    * @return
    */
  def task8(): Long = {
    // TODO Task #8: How many unique numbers are in ListOfAustralianTreaties.txt? 
    val australianTreatiesRdd = Loader.loadAustralianTreaties(sc)
    val answer8 = Tokenizer.numbers(australianTreatiesRdd).distinct().count()
    answer8
  }

  /**
    * Task #9: Calculate average of all numbers in ListOfAustralianTreaties.txt? 
    * i.e. string "1842 – Treaty 5 March 1856)[5]» has average 927
    *
    * @return average value for ListOfAustralianTreaties.txt
    */
  def task9(): Double = {
    // TODO Task #9: Calculate average of all numbers in ListOfAustralianTreaties.txt? 
    val australianTreatiesRdd = Loader.loadAustralianTreaties(sc)
    val answer9 = Tokenizer.numbers(australianTreatiesRdd).sum()/Tokenizer.numbers(australianTreatiesRdd).count()
    answer9
  }

  /**
    * Task #11: What are 10 most frequent symbols in ListOfAustralianTreaties.txt?
    * Hint: use wordFrequency()
    * Hint: the result should be sorted in descending way
    *
    * @return
    */
  def task11: Seq[String] = {
    // TODO Task #11: What are 10 most frequent symbols in ListOfAustralianTreaties.txt?
    val australianTreatiesRdd = Loader.loadAustralianTreaties(sc)
    val answer11 = Tokenizer.wordFrequency(australianTreatiesRdd).collect().toList
    answer11.take(10)
  }

  /**
    * Task #13: How many elements there are in each group in ListOfAustralianTreaties.txt
    * Hint: use Tokenizer.classify()
    *
    * @return
    */
  def task13: Map[String, Long] = {
    // TODO Task #13: How many elements there are in each group in ListOfAustralianTreaties.txt
    ???
  }

  /**
    * Task #14: Print samples of each group with A, B, C, D values from ListOfAustralianTreaties.txt?
    * Hint: use Tokenizer.wordClassifier()
    * @return
    */
  def task14: RDD[(String, (Int, Int, Int, Int))] = {
    // TODO Task #14: Print samples of each group with A, B, C, D values from ListOfAustralianTreaties.txt?
    ???
  }

}
