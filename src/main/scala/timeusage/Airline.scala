package timeusage

case class Airline(YEAR:String,MONTH:String,DAY_OF_MONTH:String,DAY_OF_WEEK:String,
                   CARRIER:String,FL_NUM:String,ORIGIN:String,DEST:String,
                   DEP_TIME:String,DEP_DELAY:String,ARR_TIME:String,ARR_DELAY:String,
                   CANCELLED:String,CANCELLATION_CODE:String,AIR_TIME:String,DISTANCE:String)

object Airline {

  def parse(line: String): Airline = {

    val columns = line.split(",")

    Airline(columns(0), columns(1), columns(2), columns(3),
      columns(4), columns(5), columns(6), columns(7),
      columns(8), columns(9), columns(10), columns(11),
      columns(12), columns(13), columns(14), columns(15))

  }
}
