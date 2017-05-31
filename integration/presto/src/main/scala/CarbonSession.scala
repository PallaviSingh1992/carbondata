import java.io.File

import org.apache.spark.sql.SparkSession


object CarbonSession {
  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/presto/target/store"
    val warehouse = s"$rootPath/integration/presto/target/warehouse"
    val metastoredb = s"$rootPath/integration/presto/target"
    import org.apache.spark.sql.CarbonSession._

    val carbon = SparkSession
      .builder()
      .master("local")
      .appName("HiveExample")
      .config("carbon.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    // For implicit conversions like converting RDDs to DataFrames


    //    println(spark.sparkContext)

  }
}

