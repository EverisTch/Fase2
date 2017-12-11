package com.tchile.bigdata

import scala.io.Source
import javax.xml.bind.DatatypeConverter
import java.security.{MessageDigest, NoSuchAlgorithmException}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.log4j.{Level, Logger}
import org.apache.log4j.PropertyConfigurator
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path

object MobileLine {

  val loggerName  = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  PropertyConfigurator.configure("log4j.properties");
  
  def main(args: Array[String]): Unit = {

    logger.info("--------------------------------------")
    logger.info("El proceso ha comenzado correctamente!")
    logger.info("--------------------------------------")
    
    val conf_file   = args(0)
    val algorithm   = "MD5"
    val salt        = "c4d4"

    try{           
        // obtain list values  
        logger.info("Asignando variables de configuración provistas por el usuario")
        val listValues = obtainValuesFromFile(conf_file)
        if (listValues.size < 10){
          
          println("-------------------------------------------------------------------------------")
          println("Parámetros insuficientes en archivo de configuración (Clave/Valor)  						")
          println("-------------------------------------------------------------------------------")
          println("nomSparkContxt	= yarn                                                          ")
          println("nomApp 				= mobile_line_digest                                            ")
          println("esquema        = test                                                          ")
          println("nomtabla       = mobile_line                                                   ")
          println("input_values   = billing_cycle_id,billing_account_id,geo_area_id               ")
          println("colpartition   = time                                                          ")
          println("servidorHDFS   = hdfs://chs-yqz-853-mn002.bi.services.us-south.bluemix.net:8020")
          println("schemaHDFS			= urm																														")
          println("userHDFS				= rhmaripa																											")
          println("nameFileHDFS		= Mobile_Line																										")
          println("-------------------------------------------------------------------------------")
        
        }else{
          
          // we asign variables
          val nomSparkContxt = listValues(0)
          val nomApp         = listValues(1)
          val esquema        = listValues(2)
          val nomtabla       = listValues(3)
          val listFields     = listValues(4).split(",").toList
          val colpartition   = listValues(5)
          val servidorHDFS   = listValues(6)
          val schemaHDFS     = listValues(7)
          val userHDFS       = listValues(8)
          val nameFileHDFS   = listValues(9)
          
          // we start the program
          logger.info("Generando variables de contexto de Spark")
          val conf       = new SparkConf().setMaster(nomSparkContxt).setAppName(nomApp)
          val sc         = new SparkContext(conf)
          val sqlContext = new SQLContext(sc)
          //import sqlContext.implicits._
  
          // check list values with hive
          logger.info("Comparando columnas a encriptar con tabla de destino en Hive")
          val listDigest     = checkListDigest(sqlContext, listFields, esquema, nomtabla)
          
          // obtain list all columns in hive
          val listColTabla   = obtainAllColumnsHive(sqlContext, esquema, nomtabla)
          
          // creating directories 06-12-2017
          val listDirectories = generateDirectories(sc, servidorHDFS, schemaHDFS, userHDFS, nameFileHDFS)
          
          // copy files to landing
          val (path_hdfs, path_hdfs_out) = copyFiletoLanding(sc, servidorHDFS, listDirectories, userHDFS, nameFileHDFS)
          
          // input files equals output files
          if (path_hdfs.size == path_hdfs_out.size){
            for(i <- 0 until path_hdfs.size){
              // generate dataset
              logger.info("Generando DataFrame con las columnas requeridas encriptadas")
              val dfEncriptado   = generateDataset(sc, sqlContext, listColTabla, listDigest, colpartition, path_hdfs(i), algorithm, salt)
              //dfEncriptado.show()
              
              // check output file
              logger.info("Validando que la carpeta HDFS de destino no exista")
              checkOutput(sc, path_hdfs_out(i), servidorHDFS)
              
              logger.info("Guardando salida encriptada con BZip2Codec")
              val RDDoutput      = dfEncriptado.rdd
              RDDoutput.repartition(1).saveAsTextFile(path_hdfs_out(i), classOf[BZip2Codec])   
            }
            logger.info("--------------------------------------")
            logger.info("El proceso ha terminado correctamente!")
            logger.info("--------------------------------------")
          }

        }
  }catch{
      case e: Exception => {
        logger.error("--------------------------------------")
        logger.error(e.printStackTrace)
        logger.error("--------------------------------------")
      }
  }
    
  }

  def generateDirectories(sc: SparkContext, servidorHDFS: String, p_schema: String, p_user: String, p_nameFile: String) : List[String] = {
  
  	val schema       = p_schema.toLowerCase
  	val user 		     = p_user.toLowerCase
  	val nameFile	   = p_nameFile.toLowerCase
  
  	// dynamic parameters
  	val directories  = "/user/".concat(user).concat("/").concat(nameFile).concat("/landing,/user/").concat(user).concat("/").concat(nameFile).concat("/processing,/user/").concat(user).concat("/cl/").concat(nameFile).concat(",/stage/").concat(schema).concat("/").concat(nameFile).concat("/archive").split(',')
  	val fs 			     = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(servidorHDFS), sc.hadoopConfiguration)
  	var listFiles    = List[String]()
  
  	for (i <- 0 until directories.size) 
  	{
  		val fsPath       = new Path(servidorHDFS + "/" + directories(i))
  		
  		// creating directories
  		if (!fs.isDirectory(fsPath))
  		{
  			fs.mkdirs(fsPath)
  			println("Directorio " + directories(i) + " creado")
  		}
  		else
  		{
  			println("Directorio " + directories(i) + " ya existe")	
  		}
  
  		// list of files from /stage/${schema}/${nameFile}/archive
  		if (directories(i) == "/stage/".concat(schema).concat("/").concat(nameFile).concat("/archive")) 
  		{
  			val fileListIterator = fs.listFiles(fsPath, true)
  			
  			while(fileListIterator.hasNext()) 
  			{
  				val pathFile = fileListIterator.next().getPath().toString
  				listFiles = pathFile +: listFiles
  			}
  		}
  	}
  	listFiles.reverse	
  }
  
  
  /// 2) Copiar data a landing
  // copy files from /stage/${schema}/${nameFile}/archive to /user/${user}/${nameFile}/landing
  def copyFiletoLanding(sc: SparkContext, servidorHDFS: String, listFiles: List[String], p_user: String, p_nameFile: String) : (List[String], List[String]) = {
  
    var listDirectories   = List[String]()
    var listDirectoriesIn = List[String]()
    val fs 			          = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(servidorHDFS), sc.hadoopConfiguration)
    val user              = p_user
    val nameFile          = p_nameFile
    
  	for (i <- 0 until listFiles.size) {
  		val listFiles2    = listFiles(i).split('/')
  		val sizeList   	  = listFiles2.size - 1
  		val inpFileStream = fs.open(new Path(listFiles(i)))
  		val outFileStream = fs.create(new Path(servidorHDFS + "/user/" + user + "/" + nameFile + "/landing/" + listFiles2(sizeList)))
  		IOUtils.copy(inpFileStream, outFileStream)
  		inpFileStream.close()
  		outFileStream.close()
  		val path_target   = servidorHDFS.concat("/user/").concat(user).concat("/").concat(nameFile).concat("/landing/").concat(listFiles2(sizeList))
  		listDirectoriesIn = path_target +: listDirectoriesIn
  		
  		/// 4) Luego se crea dentro de /user/${user}/cl/${nameFile} 
  		///una carpeta con el rango de fechas que tiene el archivo en este caso 20171000T000000_20171000T000000
  		val regDates       = """((\d{8})(T)(\d{6})(_))""".r
  		val nomFile 	     = listFiles2(sizeList)
  		val nomDirectory   = (regDates findAllIn nomFile).mkString("_").substring(0,31)
  		val path_dir	     = "/user/" + user + "/cl/" + nameFile + "/" + nomDirectory
  
  		if (!fs.isDirectory(new Path(path_dir))) 
  		{
  			fs.mkdirs(new Path(path_dir))
  		}
  		else
  		{
  			println("Directorio ya existe")
  		}
  		listDirectories = path_dir +: listDirectories
  	}
    (listDirectoriesIn.reverse, listDirectories.reverse)
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
    var values = new Array[String](10)
    for (line <- Source.fromFile(conf_file).getLines) {
      if (line.indexOf("nomSparkContxt") == 0) values(0) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("nomApp")         == 0) values(1) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("esquema")        == 0) values(2) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("nomtabla")       == 0) values(3) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("values")         == 0) values(4) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("colpartition")   == 0) values(5) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("servidorHDFS")   == 0) values(6) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("schemaHDFS")     == 0) values(7) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("userHDFS")       == 0) values(8) = line.substring(line.indexOf("=")+1)
      if (line.indexOf("nameFileHDFS")   == 0) values(9) = line.substring(line.indexOf("=")+1)
    }
    values.toList
  }

  // obtain values from input parameter
  def obtainListValuesFromParameter(listval: String): List[String] = {
    listval.toString.split(",").toList
  }

  // check input list
  def checkListDigest(sqlContext: SQLContext, listvalues: List[String], esquema: String, nomtabla: String): List[String] = {
    val ListInput   = listvalues
    var ListDigest  = List[String]()
    val metadataTBL = sqlContext.sql("DESCRIBE "+esquema+"."+nomtabla).select("col_name").collectAsList()
    for (j <- 0 until ListInput.size) {
      for (i <- 0 until metadataTBL.size) {
        if (metadataTBL.get(i).mkString == ListInput(j)) {
          ListDigest = metadataTBL.get(i).mkString +: ListDigest
        }
      }
    }
   ListDigest
  }

  // obtain all columns from hive
  def obtainAllColumnsHive(sqlContext: SQLContext, esquema: String, nomtabla: String): List[String] = {
    var ListColumns  = List[String]()
    val metadataTBL  = sqlContext.sql("DESCRIBE "+esquema+"."+nomtabla).select("col_name").collectAsList()
    for (i <- 0 until metadataTBL.size)
    {
      ListColumns = metadataTBL.get(i).mkString +: ListColumns
    }
    ListColumns.filter(col => !col.startsWith("#") && !col.equals("")).distinct
  }

  // generateDataset
  def generateDataset(sc: SparkContext, sqlContext: SQLContext, columns: List[String], listdigest: List[String], colpartition: String, path_hdfs: String, algorithm: String, salt: String): org.apache.spark.sql.DataFrame = {
    val header       = columns.reverse
    val headerWoPart = header.filter(_ != colpartition)
    val schema       = StructType(headerWoPart.map(name => StructField(name, StringType)))
    val RDDinputFile = sc.textFile(path_hdfs).map(x => x.split('|')).map(arr => Row.fromSeq(arr))
    val dataframe    = sqlContext.createDataFrame(RDDinputFile, schema)

    sqlContext.udf.register("digest", digest _)
    var coldigest : String  = ""
    var encripting: Boolean = false
    for (i <- 0 until headerWoPart.size) {

      encripting = false
      // encripting
      for (j <- 0 until listdigest.size) {
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
