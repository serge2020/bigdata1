package com.accenture.bootcamp.day1

import org.apache.spark.rdd.RDD


object Tokenizer {

  type Word = String
  type Classifier = String
  type Amount = Long
  type WordStats = (Int, Int, Int, Int)

  /**
    * Task #3: Tokenize (split string into words) 
    * String "1842 – Treaty 5 March 1856)[5]" should consists of following words: 1842,Treaty,5,March,1856,5
    * @param line any string
    * @return
    */
  def words(line: String): Array[Word] = {
    // TODO Task #3: Tokenize (split string into words) 
    line.replaceAll("[^A-Za-z0-9]", " ").split(" ").filter(_.nonEmpty)

  }

  def tokenize(rdd: RDD[String]): RDD[Word] = rdd.flatMap(words)

  /**
    * Task #4: Count words in RDD
    * Given RDD[String]. You need tokenize it using method words() and count words
    * @param rdd input RDD
    * @return word count
    */
  def countWords(rdd: RDD[Word]): Amount = {
    // TODO Task #4: Count words in RDD

    val s = tokenize(rdd).map(word => (word,1)).reduceByKey(_+_).map(rdd => rdd._2).sum()

    s.toLong

  }

  /**
    * Task #7: Transform RDD so that it should contain numbers only
    * i.e. string "1842 – Treaty 5 March 1856)[5]" should consists of following numbers:
    * 1842, 5, 1856, 5
    *
    * @param text RDD with text
    * @return RDD with numbers only
    */
  def numbers(text: RDD[Word]): RDD[Long] = {
    // TODO Task #7: Transform RDD so that it should contain numbers only

    val t = tokenize(text)
    val regex = "[0-9]+"
    //val f = t.filter(fi => fi matches regex)
    //val f = t.filter(_.forall(_.isDigit)).map(_.toLong)
    //val f = t.map(word => word.replaceAll(regex, "")).filter(fi => fi != regex)
    //text.flatMap().replaceAll("([^0-9]\\s", " ").split(" ")
    val f = t.map(_.replaceAll("[^0-9]+", "")).filter(_.nonEmpty).map(_.toLong)
    f
  }

  def numbersAlt(text: Array[String]): Seq[Int] = {
    // TODO Task #7: Transform RDD so that it should contain numbers only

    //val regex = "[0-9]+"
    //val f = words(text.toString).filter(fi => fi matches regex).map(_.toInt)
    //val f = text.filter(x => x.matches(regex)).map(_.toInt)
    val f = text.map(_.replaceAll("[^0-9]+", "")).filter(_.nonEmpty)
    f.map(_.toInt)

    //line.replaceAll("[^A-Za-z0-9]", " ").split(" ").filter(_.nonEmpty)
  }

/*  def wordsOnly(text: RDD[Word]): RDD[(String)] = {
    // TODO Task #7: Transform RDD so that it should contain numbers only

    val t = tokenize(text)
    //val regex = "[0-9]+"
    //val f = t.filter(fi => fi matches regex)
    //val f = t.filter(_.forall(_.isDigit)).map(_.toLong)
    //val f = t.map(word => word.replaceAll(regex, "")).filter(fi => fi != regex)
    //text.flatMap().replaceAll("([^0-9]\\s", " ").split(" ")
    val f = t.map(_.replaceAll("[^A-Za-z0-9]", "")).filter(_.nonEmpty)
    val wordCount = f.map(word => (word, 1)).reduceByKey((_+_))
    val wordsSorted =  wordCount.sortBy(_._2, false).map(rdd => rdd._1)
    wordsSorted
  }*/


  /**
    * Task #10: Get word occurrences
    * Count how often each word repeats
    *
    * @return
    */
  def wordFrequency(words: RDD[Word]):RDD[(String)] = {
    // TODO Task #10: Get word occurrences
    // TODO Task #10.1: Replace output type RDD[Any] with correct one

    val t = tokenize(words)
    val f = t.map(_.replaceAll("[^A-Za-z0-9]", "")).filter(_.nonEmpty)
    val wordCount = f.map(word => (word, 1)).reduceByKey((_+_))
    val wordsSorted =  wordCount.sortBy(_._2, false).map(rdd => rdd._1)
    wordsSorted

  }

  /**
    * Task #12a: Gather word stats by 4 criteria such as:
    *   A = Number of digits
    *   B = Number of vowels
    *   C = Number of consonants
    *   D = Number of other symbols
    *
    * @param word word need to be classified
    * @return
    */
  def wordStats(word: Word): WordStats = {
    // TODO Task #12a: Gather word stats by 4 criteria
    ???
  }

  /**
    * Task #12b: Classify word statistics into 5 groups such as:
    *   Group 0: where D > 0 or A > 0 and B+C >0, name it “thrash”
    *   Group 1: where A > 0, name it “numbers”
    *   Group 2: where B == C, name it “balanced_words”
    *   Group 3: where B > C, name it “singing_words”
    *   Group 4: others, name it “grunting_words”
    * Where:
    *   A = Number of digits
    *   B = Number of vowels
    *   C = Number of consonants
    *   D = Number of other symbols
    *
    * @param wordStats word statistics (A, B, C, D)
    * @return
    */
  def wordStatsClassifier(wordStats: WordStats): Classifier = {
    // TODO Task #12b: Classify word by
    ???
  }

  def wordClassifier(word: Word): Classifier = {
    wordStatsClassifier(wordStats(word))
  }

  /**
    * Task #13a: How many elements there are in each group
    * Hint: Use wordClassifier() to implement this method
    *
    * @param words words for classification
    * @return classification
    */
  def classify(words: RDD[Word]): Map[Classifier, Amount] = {
    // TODO Task #13a: How many elements there are in each group
    ???
  }

}
