package com.cebd1.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max
import scala.math.abs
/** Find the minimum temperature by weather station */
object MinTemperatures {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }
  def parseLineV2(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val timeStamp = fields(1).toInt
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (timeStamp,entryType,temperature)
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
   
    
    // Convert to (stationID, temperature)
    val minStationTemps = minTemps.map(x => (x._1, x._3.toFloat))
   
    
    
    
        // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = minStationTemps.reduceByKey( (x,y) => min(x,y))
    
    
    
    
    // Collect, format, and print the results
    val minResults = minTempsByStation.collect()
    
    
     for (result <- minResults.sorted) {
         val station = result._1
         val temp = result._2
         val formattedTemp = f"$temp%.2f F"
         println(s"$station minimum temperature: $formattedTemp") 
      }
    
     //Modify the code to get the maximum temperature
     val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
     val maxStationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
     val maxTempsByStation = maxStationTemps.reduceByKey( (x,y) => max(x,y))
     val maxResults = maxTempsByStation.collect() 
      for (result <- maxResults.sorted) {
         val station = result._1
         val temp = result._2
         val formattedTemp = f"$temp%.2f F"
         println(s"$station maximum temperature: $formattedTemp") 
      }
      
      
    //Modify the code to list stations with the maximum difference between min and max temperature [week6 practice]  
    val combinedTempsDifferencesByStation = minTempsByStation.union(maxTempsByStation).reduceByKey( (x,y) => abs(x-y))  
    val combinedResults = combinedTempsDifferencesByStation.collect()
    for (result <- combinedResults.sorted) {
             val station = result._1
             val temp = result._2
             val formattedTemp = f"$temp%.2f F"
             println(s"$station Diff between max & min temperature: $formattedTemp") 
          }
    
    //Modify the code to find out what date has the most weather change 
    val parsedLinesV2 = lines.map(parseLineV2)
    val minTempsDate = parsedLinesV2.filter(x => x._2 == "TMIN")
    val maxTempsDate = parsedLinesV2.filter(x => x._2 == "TMAX")
    
    val minDateTemps = minTempsDate.map(x => (x._1, x._3.toFloat))
    val maxDateTemps = maxTempsDate.map(x => (x._1, x._3.toFloat))
    
    val minTempsByDateV2 = minDateTemps.reduceByKey( (x,y) => min(x,y))
    val maxTempsByDateV2 = maxDateTemps.reduceByKey( (x,y) => max(x,y))
    val combinedTempsDifferencesByDate = minTempsByDateV2.union(maxTempsByDateV2).reduceByKey( (x,y) => abs(x-y))
    
    val minDateResults = minTempsByDateV2.collect()
    val maxDateResults = maxTempsByDateV2.collect() 
    val combinedDateResults = combinedTempsDifferencesByDate.collect()
     for (result <- minDateResults.sorted) {
         val date = result._1
         val temp = result._2
         val formattedTemp = f"$temp%.2f F"
         println(s"$date minimum temperature: $formattedTemp") 
      }
      for (result <- maxDateResults.sorted) {
         val date = result._1
         val temp = result._2
         val formattedTemp = f"$temp%.2f F"
         println(s"$date maximum temperature: $formattedTemp") 
      }
      for (result <- combinedDateResults.sorted) {
         val date = result._1
         val temp = result._2
         val formattedTemp = f"$temp%.2f F"
         println(s"$date combined temperature: $formattedTemp") 
      }
      val maxDateValue = combinedDateResults.maxBy(_._2)
      val date = maxDateValue._1
      val temp = maxDateValue._2
      val tempFormatted = f"$temp%.2f F"
      print(s"Date = $date | Temp = $tempFormatted")
  
  }
}