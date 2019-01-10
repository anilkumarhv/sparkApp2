name := "sparkApp"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion= "2.2.0"

libraryDependencies+= "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies+= "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies+= "org.apache.spark" %% "spark-mllib" % "2.2.0"

// https://mvnrepository.com/artifact/com.crealytics/spark-excel
libraryDependencies += "com.crealytics" %% "spark-excel" % "0.11.0"