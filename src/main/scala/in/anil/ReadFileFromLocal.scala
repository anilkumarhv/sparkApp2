package in.anil

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ReadFileFromLocal extends App {
  val sc = new SparkContext("local", "SparkApp")
  val input = sc.textFile("C:\\Users\\ah00554631\\Desktop\\Diabetes1.csv")
  //  val data1 = input.flatMap(line => line.split(","))
  val head=input.first()
  val res = input.filter(line => line != head )
  val data1 = res.map(line => line.split(",")).map(word => Diabetes(word(0).toInt, word(1),word(2),word(3),word(4),word(5),word(6),word(7)))

  //  val result = input.flatMap(lines => lines.split(",")).map(word => (word, 1)).reduceByKey(_ + _).collect()

  //  println("result= " + result.count())

  val sqlContext = new SQLContext(sc)
//  val data = Array(
//    Diabetes("A", "A"),
//    Diabetes("B", "B")
//  )

  val df = sqlContext.createDataFrame(data1)
//  df.na.drop()
  df.show()
  df.printSchema()
  //
  //  val data = sqlContext.read.format("csv")
  //    .option("header", "true")
  //    //    .schema(schema)
  //    .load("C:\\Users\\ah00554631\\Desktop\\Diabetes1.csv")
  //  data.show()

}

case class Diabetes(No_of_times_pregnant: Int, glucose_conc: String,blood_pressure: String,
                    skin_fold_thickness: String,Hour_serum_insulin:String,BMI:String,Diabetes_pedigree_fn:String,age:String)
