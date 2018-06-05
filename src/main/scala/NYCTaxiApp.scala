import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}
import java.util.concurrent.TimeUnit

import com.esri.core.geometry.Point
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.udf
import spray.json._
import utils.{Feature, FeatureCollection}
import utils.GeoJsonProtocol._
import org.apache.spark.sql.functions._

object NYCTaxiApp {
  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("NYC Taxi Spark")
      .getOrCreate()
  }
  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  /*
    Next, we’ll need to process the pickup and dropoff times using an instance of Java’s SimpleDateFormat class with an
    appropriate formatting string to get the time in milliseconds:
   */
  def parseTaxiTime(rr: RichRow, timeField: String): Long = {
    val formatter = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val optDt = rr.getAs[String](timeField)
    optDt.map(dt => formatter.parse(dt).getTime).getOrElse(0L)
  }

  /*
    Then we will parse the longitude and latitude of the pickup and dropoff locations from Strings to Doubles using
    Scala’s implicit toDouble method, defaulting to a value of 0.0 if the coordinate is missing:
   */
  def parseTaxiLoc(rr: RichRow, locField: String): Double = {
    rr.getAs[String](locField).map(_.toDouble).getOrElse(0.0)
  }

  /*
     Helper function to parse the number of passengers per rides.
     Return -1 if an error occurs.
   */
  def parseTaxiPassenger(rr: RichRow, locField: String): Int = {
    rr.getAs[String](locField).map(_.toInt).getOrElse(-1)
  }

  def parse(row: org.apache.spark.sql.Row): Trip = {
    val rr = new RichRow(row)
    Trip(
      license = rr.getAs[String]("hack_license").orNull,
      pickupTime = parseTaxiTime(rr, "pickup_datetime"),
      dropoffTime = parseTaxiTime(rr, "dropoff_datetime"),
      pickupX = parseTaxiLoc(rr, "pickup_longitude"),
      pickupY = parseTaxiLoc(rr, "pickup_latitude"),
      dropoffX = parseTaxiLoc(rr, "dropoff_longitude"),
      dropoffY = parseTaxiLoc(rr, "dropoff_latitude"),
      passenger_count = parseTaxiPassenger(rr, "passenger_count")
    )
  }



  // Define some useful UDF
  val hours = (pickup: Long, dropoff: Long) => {
    TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
  }

  val dayOfWeek = (pickup : Long) => {
    val now = Calendar.getInstance()
    now.setTimeInMillis(pickup)
    now.get(Calendar.DAY_OF_WEEK)
  }

  val hourOfDay = (pickup : Long) => {
    val now = Calendar.getInstance()
    now.setTimeInMillis(pickup)
    now.get(Calendar.HOUR_OF_DAY)
  }



  /*
    The safe function takes an argument named f of type S => T and returns a new S => Either[T, (S, Exception)] that
    will return either the result of calling f or, if an exception is thrown, a tuple containing the invalid
    input value and the exception itself:
    */
  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }


  /*
     Parse the nyc_boroughs.geojson and wrap is into
   */
  val geojson: String = scala.io.Source.fromFile("data/nyc-boroughs.geojson").mkString
  val features: FeatureCollection = geojson.parseJson.convertTo[FeatureCollection]
  val areaSortedFeatures: IndexedSeq[Feature] = features.sortBy(f => {
    val borough = f("boroughCode").convertTo[Int]
    (borough, -f.geometry.area2D())
  })
  val bFeatures: Broadcast[IndexedSeq[Feature]] = sc.broadcast(areaSortedFeatures)

  val bLookup: (Double, Double) => String = (x: Double, y: Double) => {
    val feature: Option[Feature] = bFeatures.value.find(f => {
      f.geometry.contains(new Point(x, y)) })
    feature.map(f => { f("borough").convertTo[String]
    }).getOrElse("NA")
  }


  /*
    We will create a boroughDuration method that takes two instances of the Trip class and computes both the borough
    of the first trip and the duration in seconds between the dropoff time of the first trip and the pickup time of
    the second
   */
  def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
    val b = bLookup(t1.dropoffX, t1.dropoffY)
    val d = (t2.pickupTime - t1.dropoffTime) / 1000
    (b, d)
  }


  /*
     import, clean and prepare the data for analysis
   */
  def importAndClean(): Dataset[Trip] ={
    val taxiRaw = spark.read.option("header", "true").csv("data/trip_data_1_1000k.csv")
    taxiRaw.show(10)
    val safeParse = safe(parse)
    val taxiParsed = taxiRaw.rdd.map(safeParse)
    //taxiParsed.map(_.isLeft). countByValue(). foreach(println)
    val taxiGood = taxiParsed.map(_.left.get).toDS()
    taxiGood.cache()
    val hoursUDF = udf(hours)
    spark.udf.register("hours", hours)
    taxiGood.
      groupBy(hoursUDF($"pickupTime", $"dropoffTime").as("h")).
      count().
      sort("h").
      show()

    // Keep only the "typical" rides.
    val taxiClean = taxiGood.where("hours(pickupTime, dropoffTime) BETWEEN 0 AND 3")

    // Drop 0 0 coordinates
    val taxiDone = taxiClean.where("dropoffX != 0 and dropoffY != 0 and pickupX != 0 and pickupY != 0")

    taxiDone.cache()
  }

  def main(args: Array[String]): Unit = {

    val taxiDone = importAndClean()

    val boroughUDF = udf(bLookup)
    val dayOfWeekUDF = udf(dayOfWeek)
    val hourOfDayUDF = udf(hourOfDay)

    // Get the average dropOff location by borough
    taxiDone.
      groupBy(boroughUDF($"dropoffX", $"dropoffY").as("Borough")).
      agg(avg("dropoffX"), avg("dropoffY"), stddev("dropoffX"), stddev("dropoffY")).
      show()


    // Group by passengers and count
    val averageNumberOfPassenger = taxiDone.groupBy("passenger_count").count()
    averageNumberOfPassenger.show()



    // Sessionization
    /*
      Our goal, from many pages ago, was to investigate the relationship between the borough
      in which a driver drops his passenger off and the amount of time it takes to
      acquire another fare. At this point, the taxiDone data set contains all of the individual
      trips for each taxi driver in individual records distributed across different partitions
      of the data. To compute the length of time between the end of one ride and the start
      of the next one, we need to aggregate all of the trips from a shift by a single driver
      into a single record, and then sort the trips within that shift by time. The sort step
      allows us to compare the dropoff time of one trip to the pickup time of the next trip.
      This kind of analysis, in which we want to analyze a single entity as it executes a series
      of events over time, is called sessionization, and is commonly performed over web
      logs to analyze the behavior of the users of a website.
    */
    val sessions = taxiDone. repartition($"license"). sortWithinPartitions($"license", $"pickupTime")
    sessions.cache()

    taxiDone.select(dayOfWeekUDF($"pickupTime").as("dayOfWeek"),hourOfDayUDF($"pickupTime").as("hourOfDay"), $"passenger_count").
      groupBy("dayOfWeek", "hourOfDay").
      sum("passenger_count").
      sort("dayOfWeek", "hourOfDay").
      show(7*24)

    val boroughDurations: DataFrame = sessions.mapPartitions(trips => {
      val iter: Iterator[Seq[Trip]] = trips.sliding(2)
      val viter = iter.filter(_.size == 2).filter(p => p(0).license == p(1).license)
      viter.map(p => boroughDuration(p(0), p(1)))
    }).toDF("borough", "seconds").cache()

    boroughDurations.
      selectExpr("floor(seconds / 3600) as hours").
      groupBy("hours").
      count().
      sort("hours").
      show()

    /*
        How long it takes for a
        driver to find his next fare after a dropoff in a particular borough.
     */
    boroughDurations.
      where("seconds > 0 AND seconds < 60*60*4").
      groupBy("borough").
      agg(avg("seconds"), stddev("seconds")).
      show()
  }

  case class Trip(
                   license: String,
                   pickupTime: Long,
                   dropoffTime: Long,
                   pickupX: Double,
                   pickupY: Double,
                   dropoffX: Double,
                   dropoffY: Double,
                   passenger_count: Int
                 )

  /*
    To parse the Rows from the taxiRaw data set into instances of our case class, we will need to create some helper
    objects and functions. First, we need to be mindful of the fact that it’s likely that some of the fields in a row
    may be missing from the data, so it’s possible that when we go to retrieve them from the Row, we’ll first need to
    check to see if they are null before we retrieve them or else we’ll get an error. We can write a small helper class
    to handle this problem for any kind of Row we need to parse:
   */
  class RichRow(row: org.apache.spark.sql.Row) {
    def getAs[T](field: String): Option[T] = {
      if (row.isNullAt(row.fieldIndex(field))) {
        None
      } else {
        Some(row.getAs[T](field))
      }
    }
  }

}

