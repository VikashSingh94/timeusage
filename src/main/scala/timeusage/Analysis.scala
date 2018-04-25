package timeusage


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr



object  Analysis {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("airline")
      .config("spark.master", "local")
      .getOrCreate()


  def createDataFrame(): DataFrame = {

    val lines = spark.sparkContext.textFile(FileReader.filePath)

    val headerColumns = lines.first

    val noHeader = lines.filter(_ != headerColumns)

    val airline: RDD[Airline] = noHeader.map(line => Airline.parse(line))

    spark.createDataFrame(airline)
  }


  def calculateDelayFlights(delayFlightsDf: DataFrame):Long = {
    delayFlightsDf.count()
  }


  def calculateTimeFlights(timeFlightsDf: DataFrame):Long = {
    timeFlightsDf.count()
  }

  def calculateDelayedPercentage(flightDf: DataFrame):Long = {
    val noOfDelayFlights = calculateDelayFlights(flightDf)
    val noOfTimeFlights =  calculateTimeFlights(flightDf)

    (noOfDelayFlights * 100 )/ (noOfTimeFlights + noOfDelayFlights)
  }

  def calculateTimedPercentage(flightDf: DataFrame):Long = {
    val noOfDelayFlights = calculateDelayFlights(flightDf)
    val noOfTimeFlights =  calculateTimeFlights(flightDf)

    (noOfTimeFlights * 100 )/ (noOfTimeFlights + noOfDelayFlights)
  }



  def delayedFlightByDayOfWeek(delayFlightsDf:DataFrame): DataFrame ={
    delayFlightsDf.groupBy("DAY_OF_WEEK").count.orderBy("DAY_OF_WEEK").
      withColumnRenamed("count", "DELAYED_FLIGHT_COUNT")
  }


  def OnTimedFlightByDayOfWeek(timedFlightsDf:DataFrame): DataFrame ={
    timedFlightsDf.groupBy("DAY_OF_WEEK").count.orderBy("DAY_OF_WEEK").
      withColumnRenamed("count", "TIMED_FLIGHT_COUNT")
  }



  def addExtraColumns(delayedByDayOfWeekDf:DataFrame , timedByDayOfWeekDf:DataFrame):DataFrame={

    val analysisByDayOFWeek = delayedByDayOfWeekDf.join(timedByDayOfWeekDf, "DAY_OF_WEEK")

    val delayedPercentageExpression = "(DELAYED_FLIGHT_COUNT * 100)/(DELAYED_FLIGHT_COUNT + TIMED_FLIGHT_COUNT)"
    val timedPercentageExpression = "(TIMED_FLIGHT_COUNT * 100)/(DELAYED_FLIGHT_COUNT + TIMED_FLIGHT_COUNT)"
    val delayedByOnTimePercentageExpression = "(DELAYED_FLIGHT_COUNT /TIMED_FLIGHT_COUNT) "


    analysisByDayOFWeek.withColumn("Delayed(Percentage)",expr(delayedPercentageExpression))
                        .withColumn("OnTime(Percentage)",expr(timedPercentageExpression))
                          .withColumn("Ratio(Delayed/OnTime)",expr(delayedByOnTimePercentageExpression))

  }




  def main(args: Array[String]): Unit = {

    val dataFrame = createDataFrame().cache()

    val delayFlightsDf = dataFrame.filter("ARR_DELAY  != 'NA'").filter(dataFrame("ARR_DELAY") > 0).cache()

    val timedFlightsDf = dataFrame.filter("ARR_DELAY  != 'NA'").filter(dataFrame("ARR_DELAY").equalTo(0)).cache()

    println(calculateDelayFlights(delayFlightsDf))
    println(calculateTimeFlights(timedFlightsDf))


    val delayedByDayOfWeekDf = delayedFlightByDayOfWeek(delayFlightsDf)
    val timedByDayOfWeekDf = OnTimedFlightByDayOfWeek(timedFlightsDf)

    val analysis = addExtraColumns(delayedByDayOfWeekDf,timedByDayOfWeekDf)

    analysis.show()

    println(calculateDelayedPercentage(delayFlightsDf))
    println(calculateTimedPercentage(timedFlightsDf))

    val hiveTable: Unit =  analysis.createOrReplaceTempView("my_temp_table")
    spark.sql("drop table if exists my_table")
    spark.sql("create table my_table as select * from my_temp_table")

  }
}

































//    delayedByDayOfWeekDf.show()
//    timedByDayOfWeekDf.show()

