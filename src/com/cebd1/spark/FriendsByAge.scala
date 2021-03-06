package com.cebd1.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {
  
  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the age and numFriends fields, and convert to integers
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      // Create a tuple that is our result.
      (age, numFriends)
  }
  
  
  def parseLineByNameAndNumFriends(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the age and numFriends fields, and convert to integers
      val name = fields(1)
      val numFriends = fields(3).toInt
      // Create a tuple that is our result.
      (name, numFriends)
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    val conf = new  SparkConf().setMaster("local[*]").setAppName("FriendsByAge").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named FriendsByAge
    // alternative: val sc = new SparkContext("local[*]", "FriendsByAge")
    val sc = new SparkContext(conf)
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("../ml-100k/fakefriends.csv")
    
    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)
    
    // Lots going on here...
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
   
    
    // So now we have tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age.
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
   
    
    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val resultsByAge = averagesByAge.collect()
    
    // Sort and print the final results.
    resultsByAge.sorted.foreach(println)
   
    //Average by first name
    val rdd2 = lines.map(parseLineByNameAndNumFriends)
    val totalsByName = rdd2.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    val averageByName = totalsByName.mapValues(x => x._1 / x._2)
    val resultsByName = averageByName.collect()
    resultsByName.sorted.foreach(println)
    
    //Group by age category (30, 40, 50, ..) and average friends of each category

    val grouped = averagesByAge.map(x => (x._1, x._2, {
        if (x._1 <= 20) {20} 
        else if (x._1 <= 30) {30} 
        else if (x._1 <= 40) {40} 
        else if (x._1 <= 50) {50} 
        else if (x._1 <= 60) {60} 
        else if (x._1 <= 70) {70}
      }
    ))
    val newGrouped = grouped.map(x => (x._3,x._2))
    
    val grouped2 = newGrouped.reduceByKey( (x,y) => (x + y)).collect()
    grouped2.foreach(println)
  }
    
}
  