package in.anil

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadDataFromExcel extends App {

  val sparkSession: SparkSession = SparkSession.builder().master("local").appName("Read From Excel").getOrCreate()

  val report1 = "C:\\Users\\ah00554631\\Desktop\\Sample 18.05 RNC104 Report01.xlsx"
  val report2 = "C:\\Users\\ah00554631\\Desktop\\Sample 18.05 RNC104 Report02.xlsx"

  val report1DF = readExcelFile(report1)
  report1DF.show()

  val report2DF = readExcelFile(report2)
  report2DF.show()

  def readExcelFile(fileName: String): DataFrame = {
    sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "Sheet1")
      .option("useHeader", "true")
      .load(fileName)
  }


}
