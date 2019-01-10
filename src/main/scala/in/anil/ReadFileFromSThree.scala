package in.anil

import org.apache.spark.sql.SparkSession

object ReadFileFromSThree extends App {
  val sparkSession = SparkSession.builder.master("local[*]").appName("Diabetes").getOrCreate()

  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAIE6HTFMDRRWHPCPQ")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

  val diabetesDF = sparkSession.read
    .option("header", value = true)
    .option("delimiter", ",")
    .csv("C:\\Users\\ah00554631\\Desktop\\Diabetes1.csv")
  //.csv("D://Diabetes.csv").show()

//  diabetesDF.show()
  diabetesDF.createOrReplaceTempView("dab")

  val headers = Array("No.of_times_pregnant", "glucose_conc", "blood_pressure",
    "skin_fold_thickness", "2-Hour_serum_insulin", "BMI", "Diabetes_pedigree_fn",
    "Age", "Is_Diabetic")
  val str=headers.mkString(",")

//  diabetesDF.filter("age is null").show()
//diabetesDF.columns.map(d=>col(d).)
//  diabetesDF.sparkSession.sqlContext.sql("select * from dab where concat (" +str+") is not null" )
//  diabetesDF.show()
//  diabetesDF.na.drop(9)
//  val newDf=diabetesDF.filter(r=> r.)
//  newDf.show()

//  diabetesDF.na.drop(Seq(""))
}
