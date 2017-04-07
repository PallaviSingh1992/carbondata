/*
import java.io.File

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.SparkSession

object CarbonSessionExample {

  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath
  val storeLocation = s"$rootPath/examples/presto/target/store"
  val warehouse = s"$rootPath/examples/presto/target/warehouse"
  val metastoredb = s"$rootPath/examples/presto/target"

  def setUpCarbonSession(): SparkSession ={
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def createCarbonTable(spark: SparkSession): Unit ={
    spark.sql("DROP TABLE IF EXISTS carbon_table")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbon_table(
         |    shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    dateField date,
         |    charField char(5),
         |    floatField float,
         |    complexData array<string>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val path = s"$rootPath/examples/presto/src/main/resources/data.csv"

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | options('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,dateField,charField,floatField,complexData','COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    // scalastyle:on
  }

  def tearDownCarbonSession(spark: SparkSession): Unit ={
    spark.sql("DROP TABLE IF EXISTS carbon_table")
  }

  def main(args: Array[String]): Unit = {

    val spark=setUpCarbonSession()
    createCarbonTable(spark)
    //tearDownCarbonSession(spark)
  }

}
*/
