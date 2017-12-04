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

object Mobile_Line  {

  val loggerName  = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  
  def main(args: Array[String]): Unit = {

    logger.info("--------------------------------------")
    logger.info("El proceso ha comenzado correctamente!")
    logger.info("--------------------------------------")
    
    // input parameter
    // val conf_file   = "/spark/config.txt"
    val algorithm      = "MD5"
    val salt           = "c4d4"

    // parámetros de entrada
    if (args.size < 7){
      println("----------------------------------------------------------------------------------------")
      println("Parámetros insuficientes                                                                ")
      println("----------------------------------------------------------------------------------------")
      println("arg(0) - SparkContext   = yarn                                                          ")
      println("arg(1) - path_hdfs      = spark/datos_734M.txt                                          ")
      println("arg(2) - path_hdfs_out  = spark/datos_734M_output                                       ")
      println("arg(3) - esquema        = test                                                          ")
      println("arg(4) - nomtabla       = mobile_line                                                   ")
      println("arg(5) - input_values   = billing_cycle_id,billing_account_id,geo_area_id               ")
      println("arg(6) - colpartition   = time                                                          ")
      println("arg(7) - servidorHDFS   = hdfs://chs-yqz-853-mn002.bi.services.us-south.bluemix.net:8020")
      println("----------------------------------------------------------------------------------------")
      
      // register log by Log4J
      logger.error("----------------------------------------------------------------------------------------")
      logger.error("Parámetros insuficientes                                                                ")
      logger.error("----------------------------------------------------------------------------------------")
      logger.error("arg(0) - SparkContext   = yarn                                                          ")
      logger.error("arg(1) - path_hdfs      = spark/datos_734M.txt                                          ")
      logger.error("arg(2) - path_hdfs_out  = spark/datos_734M_output                                       ")
      logger.error("arg(3) - esquema        = test                                                          ")
      logger.error("arg(4) - nomtabla       = mobile_line                                                   ")
      logger.error("arg(5) - input_values   = billing_cycle_id,billing_account_id,geo_area_id               ")
      logger.error("arg(6) - colpartition   = time                                                          ")
      logger.error("arg(7) - servidorHDFS   = hdfs://chs-yqz-853-mn002.bi.services.us-south.bluemix.net:8020")
      logger.error("----------------------------------------------------------------------------------------")

    }else{
      
      logger.info("Generando variables de contexto de Spark")
      val conf       = new SparkConf().setAppName("mobile_line_digest").setMaster(args(0))
      val sc         = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._

      logger.info("Asignando variables de configuración provistas por el usuario")
      val path_hdfs      = args(1)
      val path_hdfs_out  = args(2)
      val esquema        = args(3)
      val nomtabla       = args(4)
      val input_values   = args(5)
      val colpartition   = args(6)
      val servidorHDFS   = args(7)

      try{
       
          // obtain list values
          val listValues     = obtainListValuesFromParameter(input_values)
    
          // check list values with hive
          logger.info("Comparando columnas a encriptar con tabla de destino en Hive")
          val listDigest     = checkListDigest(sqlContext, listValues, esquema, nomtabla)
          
          // obtain list all columns in hive
          val listColTabla   = obtainAllColumnsHive(sqlContext, esquema, nomtabla)
          
          // generate dataset
          logger.info("Generando DataFrame con las columnas requeridas encriptadas")
          val dfEncriptado   = generateDataset(sc, sqlContext, listColTabla, listDigest, colpartition, path_hdfs, algorithm, salt)
          dfEncriptado.show()
          
          // check output file
          logger.info("Validando que la carpeta HDFS de destino no exista")
          checkOutput(sc, path_hdfs_out, servidorHDFS)
          
          logger.info("Guardando salida encriptada con BZip2Codec")
          val RDDoutput      = dfEncriptado.rdd
          RDDoutput.repartition(1).saveAsTextFile(path_hdfs_out, classOf[BZip2Codec])
          
          logger.info("--------------------------------------")
          logger.info("El proceso ha terminado correctamente!")
          logger.info("--------------------------------------")        
          
      }catch{
          case e: Exception => {
            logger.error("--------------------------------------")
            logger.error(e.printStackTrace)
            logger.error("--------------------------------------")
          }
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

  // obtain values from conf file
  def obtainListValuesFromFile(conf_file: String): List[String] = {
    var values    :String = null
    for (line <- Source.fromFile(conf_file).getLines) {
      if (line.indexOf("values") == 0) values = line.substring(line.indexOf("=")+1)
    }
    values.toString.split(",").toList
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