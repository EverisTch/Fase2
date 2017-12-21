
import scala.io.Source
import javax.xml.bind.DatatypeConverter
import java.security.{MessageDigest, NoSuchAlgorithmException}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.io.{FileNotFoundException, IOException}

def digest(text: String, algorithm: String, salt: String) : String = {
val msg        = text + salt
val result     = MessageDigest.getInstance(algorithm).digest(msg.getBytes)
val output     = DatatypeConverter.printBase64Binary(result)
output
}

def fileNameFilter(path: Path) : Boolean = {
if (path.getName().contains("COPYING")) { return false }
else
{ val regNomFile = """((\d{8})(T)(\d{6})(_)){3}""".r
  regNomFile.findAllIn(path.getName()).mkString("")
  if (regNomFile != "") {return true} else {return false} }
}

def obtainValuesFromFile(conf_file: String, p_nom_entidad: String) : List[String] = {
	var values = new Array[String](8)
	for (line <- Source.fromFile(conf_file).getLines) {
		if ((line.indexOf("esquema_hive") != 0)) {
			val nom_tbl		= line.split('|')
			if (p_nom_entidad.toLowerCase.contains(nom_tbl(1))) {
				values = nom_tbl
			}
		}
	}
	values.toList
}

def obtenerConfigSpark(config_file: String) : List[String] = {
	var paramSpark = new Array[String](7)
	for (line <- Source.fromFile(config_file).getLines) {
		if (line.indexOf("nomSparkContxt") != 0) {
			paramSpark = line.split('|')
		}
	}
	paramSpark.toList
}

def obtainColumns(sqlContext: HiveContext, listFields: List[String], esquema: String, nomtabla: String): (List[String], List[String]) = {
val ListInput   = listFields
var ListDigest  = List[String]()
var ListColumns = List[String]()
val metadataTBL = sqlContext.sql("DESCRIBE "+esquema+"."+nomtabla).select("col_name").collectAsList()
for (j <- 0 until ListInput.size) {
  for (i <- 0 until metadataTBL.size) {
    if (metadataTBL.get(i).mkString.toLowerCase == ListInput(j).toLowerCase) {
      ListDigest = metadataTBL.get(i).mkString.toLowerCase +: ListDigest
    }
  }
}
for (i <- 0 until metadataTBL.size) {
	ListColumns = metadataTBL.get(i).mkString.toLowerCase +: ListColumns
}
// columns for digesting, All columns Hive
(ListDigest, ListColumns.filter(col => !col.startsWith("#") && !col.equals("")).distinct)
}

def copyFiletoLanding(ssc: StreamingContext, servidorHDFS: String, pathFile: String, p_user: String, p_nameFile: String) : (String, String, String) = {
// copy files from /stage/${schema}/${nameFile}/archive to /user/${user}/${nameFile}/landing
val fs 			  = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(servidorHDFS), ssc.sparkContext.hadoopConfiguration)
val user          = p_user
val nameFile      = p_nameFile
val fileName      = pathFile splitAt (pathFile lastIndexOf("/"))
val fileName2     = fileName._2.toString.substring(1)
val hdfs_in       = servidorHDFS.concat("/user/").concat(user).concat("/").concat(nameFile).concat("/landing/").concat(fileName2)
var path_dir      = ""
// copy file to landing
val inpFileStream = fs.open(new Path(pathFile))
val outFileStream = fs.create(new Path(servidorHDFS + "/user/" + user + "/" + nameFile + "/landing/" + fileName2))
IOUtils.copy(inpFileStream, outFileStream)
inpFileStream.close()
outFileStream.close()

// 4) Luego se crea dentro de /user/${user}/cl/${nameFile}
// una carpeta con el rango de fechas que tiene el archivo en este caso 20171000T000000_20171000T000000
val regDates     = """((\d{8})(T)(\d{6})(_))""".r
val nomFile 	 = fileName2
var nomDirectory = (regDates findAllIn nomFile).mkString("")
if (nomDirectory != ""){
  nomDirectory 	 = nomDirectory.substring(0,31)
  path_dir	     = "/user/" + user + "/CL/" + nameFile + "/" + nomDirectory
  if (!fs.isDirectory(new Path(path_dir))) { fs.mkdirs(new Path(path_dir)) }
  else { println("Directorio ya existe") }
}
// path_hdfs, path_hdfs_out, nom_files
(hdfs_in, path_dir, nomFile)
}

def generateDataframe(ssc        : StreamingContext, sqlContext  : HiveContext, columns  : List[String], 
			      	  listdigest : List[String], 	 colpartition: String, 	    path_hdfs: String, 
			      	  algorithm  : String,  		 salt: String) : DataFrame = {
val header       = columns.reverse
val headerWoPart = header.filter(_ != colpartition)
val schema       = StructType(headerWoPart.map(name => StructField(name, StringType)))
val RDDinputFile = ssc.sparkContext.textFile(path_hdfs).map(x => x.split("""\|""",-1)).map(arr => Row.fromSeq(arr))
val dataframe    = sqlContext.createDataFrame(RDDinputFile, schema)
sqlContext.udf.register("digest", digest _)
var coldigest : String  = ""
var encripting: Boolean = false
for (i <- 0 until headerWoPart.size) {
  encripting = false
  // encripting
  for (j <- 0 until listdigest.size) {
    if (listdigest(j) == headerWoPart(i))
    { encripting = true
      coldigest = coldigest + "digest("+listdigest(j)+",'"+algorithm+"','"+salt+"') AS "+listdigest(j) + "," }
    else if (listdigest(j) == headerWoPart(i))
    { encripting = true
      coldigest = coldigest + "digest("+listdigest(j)+",'"+algorithm+"','"+salt+"') AS "+listdigest(j) } 
  }
  // not encripting
  if (i != headerWoPart.size -1 && !encripting) { coldigest = coldigest + headerWoPart(i) + "," }
  else if (!encripting) { coldigest = coldigest + headerWoPart(i) }
}
dataframe.registerTempTable("tbl_req")
val sqlTemp    = "SELECT "+ coldigest +" FROM tbl_req"
val sqlDF      = sqlContext.sql(sqlTemp)
sqlDF
}

def generateDirectories(ssc     : StreamingContext, servidorHDFS: String, p_schema: String, 
						p_user	: String, 			p_nameFile  : String) : Unit = {
val schema       = p_schema.toLowerCase
val user 		 = p_user.toLowerCase
val nameFile	 = p_nameFile.toLowerCase
val directories  = "/user/".concat(user).concat("/").concat(nameFile).concat("/landing,/user/").concat(user).concat("/").concat(nameFile).concat("/processing,/user/").concat(user).concat("/CL/").concat(nameFile).concat(",/stage/").concat(schema).concat("/").concat(nameFile).concat("/archive").split(',')
val fs 			 = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(servidorHDFS), ssc.sparkContext.hadoopConfiguration)
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
  else { println("Directorio " + directories(i) + " ya existe") }
}
}

def getFileStreaming(path: String, p_config_file: String, p_conf_file_spark: String, p_log4j: String): Unit = {

lazy val logger = Logger.getLogger(getClass.getName)
logger.setLevel(Level.INFO)
PropertyConfigurator.configure(p_log4j)

var fileName       = ""
val conf_file      = p_config_file
val algorithm      = "MD5"
val salt           = "c4d4"
val inputDirectory = path

// variables de configuración Spark
val listValues 	   = obtenerConfigSpark(p_conf_file_spark)
val nomSparkContxt = listValues(0).toLowerCase
val nomApp         = listValues(1).toLowerCase
val colaYarn	   = listValues(2).toLowerCase
val cntWorkers     = listValues(3).toLowerCase
val cntCores	   = listValues(4).toLowerCase
val cntMemoria     = listValues(5).toLowerCase

// variables de contextos
val sparkConf 	   = new SparkConf().setMaster(nomSparkContxt)
									.setAppName(nomApp)
									.set("spark.yarn.queue", 		 colaYarn)
									.set("spark.executor.instances", cntWorkers)
									.set("spark.executor.cores", 	 cntCores)
									.set("spark.executor.memory", 	 cntMemoria)
val ssc 		   = new StreamingContext(sparkConf, Seconds(2))
val hiveContext    = new HiveContext(ssc.sparkContext)
val lines 		   = ssc.fileStream [LongWritable, Text, TextInputFormat](inputDirectory, x=>fileNameFilter(x), true).map{case (x, y) => (x.toString, y.toString)}
lines.foreachRDD{
	rdd =>
	{
	
      if(rdd.toDebugString.contains("hdfs"))
      {
        fileName = (rdd.toDebugString.substring(rdd.toDebugString.indexOf("hdfs"), rdd.toDebugString.indexOf("NewHadoopRDD")-1)).trim()
        println("Procesando Archivo: " + fileName)
		val fileName1     = fileName splitAt (fileName lastIndexOf("/"))
		val fileName2     = fileName1._2.toString.substring(1)

		val listaParams   = obtainValuesFromFile(conf_file, fileName2)
		val esquema_hive  = listaParams(0).toLowerCase
		val nomtabla_hive = listaParams(1).toLowerCase
		val col_encrpt    = listaParams(2).split(",").toList
		val colpartition  = listaParams(3).toLowerCase
		val servidorHDFS  = listaParams(4).toLowerCase
		val schemaHDFS    = listaParams(5).toLowerCase
		val userHDFS      = listaParams(6).toLowerCase
		val nameFileHDFS  = listaParams(7).toLowerCase

		try {
			// variables de resultado
			println("Obtener columnas de Hive")
			logger.info("Obtener columnas de Hive")
			val (listDigest, listColumns) 			  = obtainColumns(hiveContext, col_encrpt, esquema_hive, nomtabla_hive)
	        println("Copiando archivos a Landing")
	        logger.info("Copiando archivos a Landing")
	        val (path_hdfs, path_hdfs_out, nom_files) = copyFiletoLanding(ssc, servidorHDFS, fileName, userHDFS, nameFileHDFS)
		
			// generar dataframe
			println("Generando DataFrame")
			logger.info("Generando DataFrame")
			val df = generateDataframe(ssc, hiveContext, listColumns, listDigest, colpartition, path_hdfs, algorithm, salt)
			
			// elimina carpeta en el caso que exista
			println("Validando que archivo exista")
			logger.info("Validando que archivo exista")
			val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(servidorHDFS), ssc.sparkContext.hadoopConfiguration)
			fs.delete(new org.apache.hadoop.fs.Path(path_hdfs_out),true)	

			// genera directorios necesarios
			println("Generando directorios necesarios")
			logger.info("Generando directorios necesarios")
			generateDirectories(ssc, servidorHDFS, schemaHDFS, userHDFS, nameFileHDFS)

			// almacena df encriptado
			println("Almacenando archivo comprimido en BZip2")
			logger.info("Almacenando archivo comprimido en BZip2")
			df.rdd.coalesce(1).saveAsTextFile(path_hdfs_out+ "/" + nom_files, classOf[org.apache.hadoop.io.compress.BZip2Codec])
			println("Archivo " + nom_files + " procesado y almacenado")
		}catch{case e: Exception => println(e.printStackTrace())}
      }
	}
}
ssc.start()
ssc.awaitTermination()
}

def main (args: Array[String]): Unit = {
//args(0) => path de origen
//args(1) => archivo de configuración Parámetros de entrada
//args(2) => archivo de configuración Spark
//args(3) => archivo de configuración log4j
getFileStreaming("test_archive/","config_param.txt","config_spark.txt","log4j.properties")
}

main(null)
