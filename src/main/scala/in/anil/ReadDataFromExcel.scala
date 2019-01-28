package in.anil

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory


object ReadDataFromExcel extends App {

  val LOGGER = LoggerFactory.getLogger(this.getClass)

  val sparkSession: SparkSession = SparkSession.builder().master("local").appName("Read From Excel").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  val report1 = "C:\\Users\\ah00554631\\Desktop\\sample.xlsx"
  val report2 = "C:\\Users\\ah00554631\\Desktop\\sample1.xlsx"
  //  val report1 = "C:\\Users\\ah00554631\\Desktop\\Sample 18.05 RNC104 Report01.xlsx"
  //  val report2 = "C:\\Users\\ah00554631\\Desktop\\Sample 18.05 RNC104 Report02.xlsx"

  val report1DF = readExcelFile(report1)
  //  report1DF.show()

  //  val col=changeColumnNames(report1DF)
  //  val newDf=report1DF.toDF(col:_*)

  //newDf.printSchema()
  //  report1DF.printSchema()

  val emptyColumns = getEmptyColNames(report1DF)

  //  println("=====column names=========")
  //  emptyColumns.foreach {
  //    println
  //  }
  //  println("=====end==================")

  //  report1DF.na.drop(counts).show()
  //  report1DF.na.drop(Seq("pmTotNoRrcConnectReqPsSucc")).show()

  val result1DF = report1DF.drop(emptyColumns: _*)
  //  result1DF.show()
  result1DF.createOrReplaceTempView("reportOne")
  val reportOne = result1DF.as("reportOne")
  reportOne.show()


  //  val allColNames: Array[String] = report1DF.columns
  //  report1DF.select(allColNames:_*)


  val report2DF = readExcelFile(report2)
  val reportTwoEmptyColumns = getEmptyColNames(report2DF)
  val result2DF = report2DF.drop(reportTwoEmptyColumns: _*)
  result2DF.createOrReplaceTempView("reportTwo")
  val reportTwo = result2DF.as("reportTwo")
  reportTwo.show()

  //    val joinReportDF=sparkSession.sql("SELECT * FROM reportOne as r1 natural join reportTwo as r2 as r2 ON r1.Short name=r2.Short name ORDER by r1.Unnamed: 1")
  //    joinReportDF.show()
  //  val finalDF = result1DF.join(result2DF, result1DF("Short name") === result2DF("Short name") && result1DF("Unnamed: 1") === result2DF("Unnamed: 1")).drop(result2DF("Short name")).drop(result2DF("Unnamed: 1"))
  //  finalDF.show()

  val finalJoinResultDF=result1DF.join(result2DF, Seq("Short name", "Unnamed: 1"),"fullouter").orderBy("Unnamed: 1").dropDuplicates()


  val window = Window.orderBy("Unnamed: 1")
  val spec: WindowSpec = window.rangeBetween(Window.unboundedPreceding, Window.currentRow)
  //  val data = joinedDF.withColumn("COL1-comparison", compareCol1(lag("3G_DPCR_HSDPA", 1), lead("3G_DPCR_HSDPA", 1)).over(window))


  //  new code
  LOGGER.info(">>>>> start <<<<<")
  val joinRes2 = finalJoinResultDF.withColumn("3G_DPCR_HSDPA-New", meanValueOfMiss("3G_DPCR_HSDPA1"))
  val finalResult1 = joinRes2.withColumn("3G_DPCR_HSDPA1", coalesce(joinRes2("3G_DPCR_HSDPA1"), joinRes2("3G_DPCR_HSDPA-New")))
  finalResult1.select("3G_DPCR_HSDPA1", "3G_DPCR_HSDPA-New").show()
  finalResult1.show()
  finalResult1.printSchema()

  //  val joinRes = result1DF.withColumn("3G_DPCR_HSDPA-New", when(col("3G_DPCR_HSDPA").isNull, (lag("3G_DPCR_HSDPA", 1).over(window) + lead("3G_DPCR_HSDPA", 1).over(window)) / 2).otherwise(lag("3G_DPCR_HSDPA", 0).over(window)))
  //  joinRes.select("3G_DPCR_HSDPA", "3G_DPCR_HSDPA-New").show()

  //  val wSpec = Window.partitionBy("Short name").orderBy("Unnamed: 1").rowsBetween(-1, 1)
  //  val abc= mean(result1DF("3G_DPCR_HSDPA")).over(wSpec)
  //  val report5 = result1DF.withColumn("mean",when(col("3G_DPCR_HSDPA").isNull,abc).otherwise(col("3G_DPCR_HSDPA")))
  //  val finalResult=report5.withColumn("3G_DPCR_HSDPA",coalesce(report5("3G_DPCR_HSDPA"),report5("mean")))
  //  finalResult.select("3G_DPCR_HSDPA", "mean").show()
  //
  //  val filterCond = finalResult.columns.map(x=>col(x).isNull).reduce(_ && _)
  ////  finalResult.filter(filterCond).show()
  //  finalResult.where(filterCond).show()

  LOGGER.info(">>> end <<<")
  //  end

  def readExcelFile(fileName: String): DataFrame = {
    sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "Sheet1")
      .option("useHeader", "true")
      .load(fileName)
  }

  def getEmptyColNames(df: DataFrame): Seq[String] = {
    df.cache()
    val colNames: Seq[String] = df.columns
    colNames.filter { colName: String =>
      df.filter(df(colName).isNotNull).count() == 0
    }
    //    colNames.map(x => x.replace(",", "_"))
  }

  def changeColumnNames(df: DataFrame): Seq[String] = {
    df.cache()
    val colNames: Seq[String] = df.columns
    colNames.map(x => x.replace(",", "_"))
  }

  def getCurretCol(curr: Column): Column = {
    if (curr.isNotNull.expr.nullable) {
      curr.isNotNull
    } else {
      curr.isNull
    }
  }

  def meanValueOfMiss(colName: String): Column = {
    val curr = col(colName)
    val prev = lag(colName, 1).over(window)
    val next = lead(colName, 1).over(window)
    when(curr.isNull, (prev + when(next.isNotNull,next).otherwise(next)) / 2).otherwise(curr)
  }
}
