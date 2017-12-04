package com.tchile.bigdata

import scala.io.Source
import javax.xml.bind.DatatypeConverter
import java.security.{MessageDigest, NoSuchAlgorithmException}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.log4j.{Level, Logger}
import org.apache.log4j.PropertyConfigurator

object MobileLine {

  val loggerName  = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  PropertyConfigurator.configure("log4j.properties");
  
  def main(args: Array[String]): Unit = {

    logger.info("--------------------------------------")
    logger.info("El proceso ha comenzado correctamente!")
    logger.info("--------------------------------------")
    
    //val conf_file = "/spark/config.txt"
    val conf_file   = args(0)
    val algorithm   = "MD5"
    val salt        = "c4d4"

    try{           
        // obtain list values  
        logger.info("Asignando variables de configuración provistas por el usuario")
        val listValues = obtainValuesFromFile(conf_file)
        if (listValues.size < 8){
          
          println("-------------------------------------------------------------------------------")
          println("Parámetros insuficientes en archivo de configuración (Clave/Valor)  						")
          println("-------------------------------------------------------------------------------")
          println("nomSparkContxt	= yarn                                                          ")
          println("nomApp 				= mobile_line_digest                                            ")
          println("path_hdfs      = spark/datos_734M.txt                                          ")
          println("path_hdfs_out  = spark/datos_734M_output                                       ")
          println("esquema        = test                                                          ")
          println("nomtabla       = mobile_line                                                   ")
          println("input_values   = billing_cycle_id,billing_account_id,geo_area_id               ")
          println("colpartition   = time                                                          ")
          println("servidorHDFS   = hdfs://chs-yqz-853-mn002.bi.services.us-south.bluemix.net:8020")
          println("-------------------------------------------------------------------------------")
        
        }else{
          
          // we asign variables
          val nomSparkContxt = listValues(0)
          val nomApp         = listValues(1)
          val path_hdfs      = listValues(2)
          val path_hdfs_out  = listValues(3)
          val esquema        = listValues(4)
          val nomtabla       = listValues(5)
          val listFields     = listValues(6).split(",").toList
          val colpartition   = listValues(7)
          val servidorHDFS   = listValues(8)
  
          // we start the program
          logger.info("Generando variables de contexto de Spark")
          val conf       = new SparkConf().setMaster(nomSparkContxt).setAppName(nomApp)
          val sc         = new SparkContext(conf)
          val sqlContext = new org.apache.spark.sql.SQLContext(sc)
          import sqlContext.implicits._
  
          // check list values with hive
          logger.info("Comparando columnas a encriptar con tabla de destino en Hive")
          val listDigest = checkListDigest(sqlContext, listFields, esquema, nomtabla)
          
          // obtain list all columns in hive
          val listColTabla   = obtainAllColumnsHive(sqlContext, esquema, nomtabla)
          
          // generate dataset
          logger.info("Generando DataFrame con las columnas requeridas encriptadas")
          val dfEncriptado   = generateDataset(sc, sqlContext, listColTabla, listDigest, colpartition, path_hdfs, algorithm, salt)
          //dfEncriptado.show()
          
          // check output file
          logger.info("Validando que la carpeta HDFS de destino no exista")
          checkOutput(sc, path_hdfs_out, servidorHDFS)
          
          logger.info("Guardando salida encriptada con BZip2Codec")
          val RDDoutput      = dfEncriptado.rdd
          RDDoutput.repartition(1).saveAsTextFile(path_hdfs_out, classOf[BZip2Codec])
          
          logger.info("--------------------------------------")
          logger.info("El proceso ha terminado correctamente!")
          logger.info("--------------------------------------")    
        }
  }catch{
      case e: Exception => {
        logger.error("--------------------------------------")
        logger.error(e.printStackTrace)
        logger.error("--------------------------------------")
      }
  }
    
  }

  // check file in HDFS if exists we delete it
  def checkOutput(sc: SparkContext, path_hdfs_out: String, servidor: String) : Unit = {
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(servidor), sc.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path(path_hdfs_out),true)
  }

  // function for encripting
  def digest(text: String, algorithm: String, salt: String): String = {
    val msg        = text + salt
    val result     = MessageDigest.getInstance(algorithm).digest(msg.getBytes)
    val output     = DatatypeConverter.printBase64Binary(result)
    output
  }
  
  // obtain parameters from con file
  def obtainValuesFromFile(conf_file: String): List[String] = {
    var values = new Array[String](9)
    for (line <- Source.fromFile(conf_file).getLines) {
      if (line.indexOf("nomSparkContxt") == 0) values(0) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("nomApp")         == 0) values(1) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("path_hdfs")      == 0) values(2) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("path_hdfs_out")  == 0) values(3) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("esquema")        == 0) values(4) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("nomtabla")       == 0) values(5) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("values")         == 0) values(6) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("colpartition")   == 0) values(7) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("servidorHDFS")   == 0) values(8) = line.substring(line.indexOf("=")+1)
    }
    values.toList
  }

  // obtain values from input parameter
  def obtainListValuesFromParameter(listval: String): List[String] = {
    listval.toString.split(",").toList
  }

  // check input list
  def checkListDigest(sqlContext: org.apache.spark.sql.SQLContext, listvalues: List[String], esquema: String, nomtabla: String): List[String] = {
    val ListInput   = listvalues
    var ListDigest  = List[String]()
    val metadataTBL = sqlContext.sql("DESCRIBE "+esquema+"."+nomtabla).select("col_name").collectAsList()
    for (j <- 0 until ListInput.size -1) {
      for (i <- 0 until metadataTBL.size -1) {
        if (metadataTBL.get(i).mkString == ListInput(j)){
          ListDigest = metadataTBL.get(i).mkString +: ListDigest
        }
      }
    }
   ListDigest
  }

  // obtain all columns from hive
  def obtainAllColumnsHive(sqlContext: org.apache.spark.sql.SQLContext, esquema: String, nomtabla: String): List[String] = {
    var ListColumns  = List[String]()
    val metadataTBL  = sqlContext.sql("DESCRIBE "+esquema+"."+nomtabla).select("col_name").collectAsList()
    for (i <- 0 until metadataTBL.size -1)
    {
      ListColumns = metadataTBL.get(i).mkString +: ListColumns
    }
    ListColumns.filter(col => !col.startsWith("#") && !col.equals("")).distinct
  }

  // generateDataset
  def generateDataset(sc: SparkContext, sqlContext: org.apache.spark.sql.SQLContext, columns: List[String], listdigest: List[String], colpartition: String, path_hdfs: String, algorithm: String, salt: String): org.apache.spark.sql.DataFrame = {
    val header     = columns
    val headerWoPart = header.filter(_ != colpartition)
    val schema     = StructType(headerWoPart.map(name => StructField(name, StringType)))
    val RDDinputFile = sc.textFile(path_hdfs).map(x => x.split('|')).map(arr => Row.fromSeq(arr))
    val dataframe    = sqlContext.createDataFrame(RDDinputFile, schema)

    sqlContext.udf.register("digest", digest _)
    var coldigest : String  = ""
    var encripting: Boolean = false
    for (i <- 0 until headerWoPart.size -1) {

      encripting = false
      // encripting
      for (j <- 0 until listdigest.size -1) {
        if (listdigest(j) == headerWoPart(i))
        {
          encripting = true
          coldigest = coldigest + "digest("+listdigest(j)+",'"+algorithm+"','"+salt+"') AS "+listdigest(j) + ","
        }
        else if (listdigest(j) == headerWoPart(i))
        {
          encripting = true
          coldigest = coldigest + "digest("+listdigest(j)+",'"+algorithm+"','"+salt+"') AS "+listdigest(j)
        }
      }

      // not encripting
      if (i != headerWoPart.size -1 && !encripting)
      {
        coldigest = coldigest + headerWoPart(i) + ","
      }
      else if (!encripting)
      {
        coldigest = coldigest + headerWoPart(i)
      }
    }

    dataframe.registerTempTable("tbl_req")
    val sqlTemp    = "SELECT "+ coldigest +" FROM tbl_req"
    val sqlDF      = sqlContext.sql(sqlTemp)
    sqlDF
  }
}