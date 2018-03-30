package observatory

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

trait ExtractionTest extends FunSuite {

  val z = Extraction.locateTemperatures(
    2015,
    "/Users/grigoriy.evlash/projects/observatory/src/test/resources/stations.csv",
    "/Users/grigoriy.evlash/projects/observatory/src/test/resources/2015.csv"
  )

  println(z)
  
}