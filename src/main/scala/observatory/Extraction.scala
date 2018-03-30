package observatory

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
  * 1st milestone: data extraction
  */
object Extraction {

  final val appName = "chiki-briki project"
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName(appName).setMaster("local[4]")
  val sc = new SparkContext(conf)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    val stations = sc.textFile(stationsFile).flatMap{ s =>
       Option(s.split(',').toList)
         .filter( l => l.lengthCompare(4) == 0 && l(2).nonEmpty && l(3).nonEmpty)
         .map{
           case stn :: wban :: lat :: lon :: Nil => (stn -> wban) -> Location.toLocation(lat, lon).get
       }
    }

    val temps = sc.textFile(temperaturesFile).flatMap{ s =>
      Option(s.split(',').toList)
        .filter(_.lengthCompare(5) == 0)
        .map{
          case stn :: wban :: month :: day :: temp :: Nil =>
            val t = Try(temp.toDouble).getOrElse(9999.9)
            val d = LocalDate.of(year, month.toInt, day.toInt)
            (stn, wban) ->  (d, t)
      }
    }

    stations.join(temps).map{
      case (_, (location, (d, t))) => (d, location, t )
    }.collect().toSeq
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    sparkAverageRecords(sc.parallelize(records.toSeq)).collect().toSeq
  }

  def sparkAverageRecords(records: RDD[(LocalDate, Location, Temperature)]): RDD[(Location, Temperature)] = {
//    records.


    ??? // actual work done here
  }
}
