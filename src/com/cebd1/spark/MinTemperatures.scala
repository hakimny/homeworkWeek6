package com.cebd1.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max

/** Find the minimum temperature by weather station */
object MinTemperatures {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new  SparkConf().setMaster("local[*]").setAppName("MinTemperatures").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named MinTemperatures
    // alternative: val sc = new SparkContext("local[*]", "MinTemperatures")
    val sc = new SparkContext(conf)
    
    // Read each line of input data
    val lines = sc.textFile("../ml-100k/1800.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    // Filter out all but TMIN entries
    val minTemps = parsedLines.filter(x => x._2 == "TMIN")
    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
    
    // Convert to (stationID, temperature)
    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
    val maxStationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
   
    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y))
    val maxTempsByStation = maxStationTemps.reduceByKey( (x,y) => max(x,y))
     
    // Collect, format, and print the results
    val minResults = minTempsByStation.collect()
    val maxResults = maxTempsByStation.collect() 
    
    
    for (result <- minResults.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station minimum temperature: $formattedTemp") 
    }
    for (result <- maxResults.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station maximum temperature: $formattedTemp") 
    }
      
  }
}