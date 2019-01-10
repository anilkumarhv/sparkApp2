package in.anil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ReadFileFromLocalNew extends App {

  val sparkSession = SparkSession.builder().appName("SparkApp").master("local").getOrCreate()

  val input = sparkSession.read.format("csv").option("header", value = true).option("delimeter", ",").csv("C:\\Users\\ah00554631\\Desktop\\Diabetes.csv")

  //  val input=sparkSession.sparkContext.textFile("C:\\Users\\ah00554631\\Desktop\\Diabetes1.csv")
  //    val head=input.first()
  //    val res=input.filter(lines=> lines != head).rdd


  //    val headers1=input.first().toSeq.toList
  val headers = Seq("No.of_times_pregnant", "glucose_conc", "blood_pressure",
    "skin_fold_thickness", "2-Hour_serum_insulin", "BMI", "Diabetes_pedigree_fn",
    "Age", "Is_Diabetic").toList

  println(">>>>>> headers <<<<<<<")
  headers.foreach {
    println
  }
  println(">>>>>> headers <<<<<<<")

  input.columns.foreach { in => println(in) }

  val rdd = input.rdd
  val df = input.sqlContext.createDataFrame(rdd, dfSchema(headers))

  df.show()

  def dfSchema(columnNames: List[String]): StructType =
    StructType(
      Seq(
        StructField(name = "No.of_times_pregnant", dataType = StringType, nullable = false),
        StructField(name = "glucose_conc", dataType = StringType, nullable = false),
        StructField(name = "blood_pressure", dataType = StringType, nullable = false),
        StructField(name = "skin_fold_thickness", dataType = StringType, nullable = false),
        StructField(name = "2-Hour_serum_insulin", dataType = StringType, nullable = false),
        StructField(name = "BMI", dataType = StringType, nullable = false),
        StructField(name = "Diabetes_pedigree_fn", dataType = StringType, nullable = false),
        StructField(name = "Age", dataType = StringType, nullable = false),
        StructField(name = "Is_Diabetic", dataType = StringType, nullable = false)
      )
    )
}
