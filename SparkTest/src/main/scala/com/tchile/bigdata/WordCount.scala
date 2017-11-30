package com.tchile.bigdata
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object WordCount {
  def main(args: Array[String]) = {
 
    //Iniciar Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
 
    //Lee un archivo de ejemplo para probar RDD
    val test = sc.textFile("input/input.txt")
 
    test.flatMap { line => //para cada linea
      line.split(" ") //separar la linea por palabra.
    }
      .map { word => //por cada palabra
        (word, 1) // retornar clave/valor con la palabra como clave y 1 como valor
      }
      .reduceByKey(_ + _) // Sumar todos los valores con la misma clave
      .saveAsTextFile("output") //Guarda como archivo de text
 
    //Detener Spark context
    sc.stop
  }
}