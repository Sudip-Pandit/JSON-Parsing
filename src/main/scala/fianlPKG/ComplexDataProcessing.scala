package fianlPKG
import org.apache.spark._

import sys.process._
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext.jarOfObject
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import shapeless.syntax.std.tuple.unitTupleOps
import spire.implicits.eqOps
import scala.util.parsing.json.JSON
object ComplexDataProcessing {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ComplexDataProcessing").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder()
      .config("fs.s3a.access.key","")
      .config("fs.s3a.secret.key","")
      .getOrCreate()

    import spark.implicits._
    println("====JSON ComplexDataProcessing =====> Multiline =====")

    val df = spark.read.format("json").option("multiline", "true").load("file:///D:/data/zeyoc.json")
    df.show()
    df.printSchema()
    val flattendf = df.select(
      "orgname",
      "trainer",
      "address.permanentAddress",
      "address.temporaryAddress"

    )
    flattendf.show()
    flattendf.printSchema()

val df1 = spark.read.format("json").option("multiline","true").load("file:///D:/data/donut.json")
df1.show()
df1.printSchema()

    println("===JSON ComplexDataProcessing ==> StructType===")
    val flattendf2 = df1.select(
      col("id"),
      col("image.height").as("iheight"),
      col("image.url").as("iurl"),
      col("image.width").as("iwidth"),
      col("name"),
      col("thumbnail.height").as("theight"),
      col("thumbnail.url").as("url"),
      col("thumbnail.width").as("twidth"),
      col("type")
      )

    flattendf2.show()
    flattendf2.printSchema()

    val df3 = spark.read.format("json").option("multiline","true").load("file:///D:/data/zeyoc.json")

    df.show()
    df.printSchema()

    val flattendf3 = df.select(
    "address.*",
    "orgname",
    "trainer"
  )
   flattendf3.show()
    flattendf3.printSchema()

    val df4 = spark.read.format("json").option("multiline","true").load("file:///D:/data/place.json")
      df4.show()
    df4.printSchema()

    val flattendf4 = df1.select(
      col("place"),
      col("user.address.*"),
      col("user.name")
    )
    flattendf4.show()
    flattendf4.printSchema()

    val df5 = spark.read.format("json").option("multiline","true").load("file:///D:/data/topping.json")

    df5.show()

    df5.printSchema()

    val flattendf5 = df5.select(
      col("id"),
      col("name"),
      col("ppu"),
      col("batters.batter.id").as("bid"),
      col("batters.batter.type").as("btype"),
      col("topping.id").as("t.id"),
      col("topping.type").as("t.type"),
      col("type")
    )
    flattendf5.show()
    flattendf5.printSchema()

  }

}
