// import necesaries libraries
import scala.io.Source
import javax.xml.bind.DatatypeConverter
import java.security.{MessageDigest, NoSuchAlgorithmException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

object SparkMobileLine {

def main(args: Array[String]) {
	// call function for obtaining Key/Values
	val conf_file	     = "/spark/config.txt"
	val path_hdfs	     = "/spark/datos.txt"
	val algorithm        = "MD5"
	val salt 		     = "c4d4"
	val arrayTuple       = obtainParKeyValue(conf_file)
	// load data to encript
	val RDDinputFile     = sc.textFile(path_hdfs).map(x => x.split('|'))
	// call function for generate dataset
	generateDataset(RDDinputFile, path_hdfs, arrayTuple, algorithm, salt)
}

// function for encripting
def digest(text: String, algorithm: String, salt: String): String = {
    val msg 	   = text + salt;
    val result 	   = MessageDigest.getInstance(algorithm).digest(msg.getBytes)
    val output 	   = DatatypeConverter.printBase64Binary(result)
    return output   
}

// obtain pars Key Values
def obtainParKeyValue(conf_file: String): Array[(String, String)] = {
// input variables
var key	   	  :String = null
var values 	  :String = null
for (line <- Source.fromFile(conf_file).getLines) {
if(line.indexOf("key") == 0) key = line.substring(line.indexOf("=")+1)
else if(line.indexOf("values") == 0) values = line.substring(line.indexOf("=")+1)
}
// generate lists
var Listkey    = List(key)
val Listvalues = values.toString.split(",").toList
// modify list for using zip
for (i <- 1 to Listvalues.length -1){
Listkey = key +: Listkey
}
// combine list (Key, Value)
val RDDKey     = sc.parallelize(Listkey)
val RDDvalues  = sc.parallelize(Listvalues)
val ArrayTuple = RDDKey.zip(RDDvalues).collect()
return ArrayTuple
}

// generateDataset
def generateDataset(RDDinputFile: org.apache.spark.rdd.RDD[Array[String]], path_hdfs: String, arrayTuple: Array[(String, String)], algorithm: String, salt: String): Unit = {
// generate DF with columns for default
var header 	     = Seq[String]()
val cntFilas 	 = sc.textFile(path_hdfs).first.split('|').size
for (i <- 0 to cntFilas -1){
header 			 = header :+ "col"+(i).toString
}
val schema 		 = StructType(header.map(name => StructField(name, StringType)))
val rdd 	 	 = RDDinputFile.map(arr => Row.fromSeq(arr))
val dfOriginal   = sqlContext.createDataFrame(rdd, schema)
dfOriginal.registerTempTable("original")

// generate list to join
var sqlTemp 	 = "SELECT * FROM original o "
// register temporal tables 
arrayTuple.foreach{
f => val RDDtmp  = RDDinputFile.map(x => (x(f._1.toInt), digest(x(f._2.toInt),algorithm,salt)))
val DataFrameTmp = RDDtmp.toDF("key", "value")
RDDtmp.toDF().registerTempTable("DF"+f._2)
sqlTemp    += "INNER JOIN " + ("DF"+f._2) + " d"+f._2 + " ON o.col"+ f._1 +" = " + " d"+f._2 + ".key "
}
val sqlDF 		 = sqlContext.sql(sqlTemp)
sqlDF.show()
}

}
