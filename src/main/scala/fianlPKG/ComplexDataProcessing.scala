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

    df3.show()
    df3.printSchema()

    val flattendf3 = df3.select(
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

    val df6 = spark.read.format("json").option("multiline","true").load("file:///D:/data/zeyo.json")
    df6.show()
    df6.printSchema()

    val flattendf6 = df6.select(
      col("address.*"),
      col("orgname"),
      col("trainer")

    )
    flattendf6.show()
    flattendf6.printSchema()

    val complexdf = flattendf6.select(

      struct(
        col("permanentAddress"),
        col("temporaryAddress")
      ).as("address"),
      col("orgname"),
      col("trainer")


    )
    complexdf.show()
    complexdf.printSchema()

    val df7= spark.read.format("json").option("multiline","true").load("file:///C:/Users/sudip/OneDrive/Desktop/zeyoc.json")

    df7.show
    df7.printSchema()
    val flattendf7 = df7.select(
      col("address.permanentAddress.area").as("parea"),
      col("address.permanentAddress.location").as("plocation"),
      col("address.temporaryAddress.area").as("tarea"),
      col("address.temporaryAddress.location").as("tlocation"),
      col("orgname"),
      col("trainer")
    )
    flattendf7.show()
    flattendf7.printSchema()

    val complexdf7 = flattendf7.select(
      struct(
        struct(
          col("parea").as("area"),
          col("plocation").as("location")
        ).as("permanentAddress"),
        struct(
          col("tarea").as("area"),
          col("tlocation").as("location")
        ).as("temporaryAddress")

      ).as("address"),
      col("orgname"),
      col("trainer")
    )

    complexdf7.show()
    complexdf7.printSchema()

   val df8 = spark.read.format("json").option("multiline","true").load("file:///C:/Users/sudip/OneDrive/Desktop/zeyoc.json")
    df8.show()
    df8.printSchema()

    val flattendf8 = df.select(
      explode(col("students")).as("Students"),
      col("address.*"),
      col("orgname"),
      col("trainer")
    )

    flattendf8.show()
    flattendf8.printSchema()

   val petsdf9 = spark.read.format("json").option("multiline","true").load("file:///D:/data/pets.json")
    petsdf9.show()
    petsdf9.printSchema()

    val flattenpetsdf9 = petsdf9.select(
      col("Address.*"),
      col("mobile"),
      col("Name"),
      explode(col("Pets")).as("Pets"),
      col("status")
    )
    flattenpetsdf9.show()
    flattenpetsdf9.printSchema()
println("====flatten withColumn===")
    val flattenwithColumn9 = petsdf9
      .withColumn("Permanent address", expr("Address.`Permanent address`"))
        .withColumn("current Address", expr("Address.`current Address`"))
        .withColumn("Pets", expr("explode(Pets)")
        )
    flattenwithColumn9.show()
    flattenwithColumn9.printSchema()
    val df10 = spark.read.format("json").option("multiline","true").load("file:///D:/data/donut.json")

    df10.show()
    df10.printSchema()

    val flattendf11 = df10.select(
      col("id"),
      col("image.height").as("iheight"),
      col("image.url").as("iurl"),
      col("image.width").as("iwidth"),
      col("name"),
      col("thumbnail.height").as("theight"),
      col("thumbnail.url").as("turl"),
      col("thumbnail.width").as("twidth"),
      col("type")

    )
    flattendf11.show()
    flattendf11.printSchema()
val complexdf11 = flattendf11.select(
  col("id"),
  struct(
    col("iheight").as("height"),
    col("iurl").as("url"),
    col("iwidth").as("width")
  ).as("image"),
  col("name"),
  struct(
    col("iheight").as("height"),
    col("turl").as("url"),
    col("twidth").as("width")

  ).as("thumbnail"),
  col("type")

)
   complexdf11.show()

    complexdf11.printSchema()

  }

}
