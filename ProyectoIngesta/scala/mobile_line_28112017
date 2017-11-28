// import necesaries libraries
import scala.io.Source
import javax.xml.bind.DatatypeConverter
import java.security.{MessageDigest, NoSuchAlgorithmException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

// function for encripting
def digest(text: String, algorithm: String, salt: String): String = {
val msg 	     = text + salt;
val result 	   = MessageDigest.getInstance(algorithm).digest(msg.getBytes)
val output 	   = DatatypeConverter.printBase64Binary(result)
return output
}

// obtain values from conf file 
def obtainListValuesFromFile(conf_file: String): List[String] = {
var values 	  :String = null
for (line <- Source.fromFile(conf_file).getLines) {
if (line.indexOf("values") == 0) values = line.substring(line.indexOf("=")+1)
}
return values.toString.split(",").toList
}

// obtain values from input parameter
def obtainListValuesFromParameter(listval: String): List[String] = {
return listval.toString.split(",").toList
}

// check input list
def checkListDigest(listvalues: List[String], esquema: String, nomtabla: String): List[String] = {
var ListInput   = listvalues
var ListDigest	= List[String]()
var metadataTBL = sqlContext.sql("DESCRIBE "+esquema+"."+nomtabla).select("col_name").collectAsList()
for (j <- 0 to ListInput.size -1) {
for (i <- 0 to metadataTBL.size -1) {
if (metadataTBL.get(i).mkString == ListInput(j)){
ListDigest = metadataTBL.get(i).mkString +: ListDigest
}
}
}
return ListDigest
}

def obtainAllColumnsHive(esquema: String, nomtabla: String): List[String] = {
var ListColumns	 = List[String]()
var metadataTBL  = sqlContext.sql("DESCRIBE "+esquema+"."+nomtabla).select("col_name").collectAsList()
for (i <- 0 to metadataTBL.size -1) {
ListColumns = metadataTBL.get(i).mkString +: ListColumns
}
return ListColumns.filter(col => !col.startsWith("#") && !col.equals("")).distinct
}

// generateDataset
def generateDataset(columns: List[String], listdigest: List[String], colpartition: String, delim: String, path_hdfs: String, algorithm: String, salt: String): org.apache.spark.sql.DataFrame = { 
val header 		 = columns.toSeq
val headerWoPart = header.filter(_ != colpartition)
val schema 		 = StructType(headerWoPart.map(name => StructField(name, StringType)))
val RDDinputFile = sc.textFile(path_hdfs).map(x => x.split('|')).map(arr => Row.fromSeq(arr))
val dataframe    = sqlContext.createDataFrame(RDDinputFile, schema)

sqlContext.udf.register("digest", digest _)
var coldigest : String  = ""
var encripting: Boolean = false
for (i <- 0 to headerWoPart.size -1) {

encripting = false
// encripting
for (j <- 0 to listdigest.size -1) {
// j != listdigest.size -1 && 
if (listdigest(j) == headerWoPart(i)) 
{encripting = true
coldigest = coldigest + "digest("+listdigest(j)+",'"+algorithm+"','"+salt+"') AS "+listdigest(j) + ","}
else if (listdigest(j) == headerWoPart(i))
{encripting = true
coldigest = coldigest + "digest("+listdigest(j)+",'"+algorithm+"','"+salt+"') AS "+listdigest(j)}
}

// not encripting
if (i != headerWoPart.size -1 && encripting == false)
{coldigest = coldigest + headerWoPart(i) + ","}
else if (encripting == false)
{coldigest = coldigest + headerWoPart(i)}
}

dataframe.registerTempTable("tbl_req")
var sqlTemp 	 = "SELECT "+ coldigest +" FROM tbl_req"
val sqlDF 		 = sqlContext.sql(sqlTemp)
return sqlDF
}

// input parameter
val conf_file	     = "/spark/config.txt"
val path_hdfs	     = "spark/datos.txt"
val algorithm      = "MD5"
val salt 		       = "c4d4"
val esquema			   = "test"
val nomtabla		   = "mobile_line"
val input_values	 = "billing_cycle_id,billing_account_id,geo_area_id"
val colpartition	 = "time"
var delimitador		 = "'|'"

// obtain list values
val listValues  	 = obtainListValuesFromParameter(input_values)
// check list values with hive
val listDigest 		 = checkListDigest(listValues, esquema, nomtabla)
// obtain list all columns in hive
val listColTabla 	 = obtainAllColumnsHive(esquema, nomtabla)
// generate dataset
val dfEncriptado 	 = generateDataset(listColTabla, listDigest, colpartition, delimitador, path_hdfs, algorithm, salt)
dfEncriptado.show()
