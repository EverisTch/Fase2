// import necesaries libraries
import scala.io.Source
import javax.xml.bind.DatatypeConverter
import java.security.{MessageDigest, NoSuchAlgorithmException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

// function for encripting
def digest(text: String, algorithm: String, salt: String): String = {
    val msg 	   = text + salt;
    val result 	   = MessageDigest.getInstance(algorithm).digest(msg.getBytes)
    val output 	   = DatatypeConverter.printBase64Binary(result)
    return output   
}

// obtain pars Key Values
def obtainParKeyValue(conf_file: String, path_hdfs: String): Array[(String, String)] = {
	
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
def generateDataset(RDDinputFile: org.apache.spark.rdd.RDD[Array[String]], arrayTuple: Array[(String, String)], algorithm: String, salt: String): Unit = {

	// generate list to join
	var listTables   = List[String]()
	var sqlTemp 	 = "SELECT * FROM original o "

	// register original table
	// it's necessary define the schema to original table...

	// register temporal tables 
	arrayTuple.foreach{
	f => val RDDtmp  = RDDinputFile.map(x => (x(f._1.toInt), digest(x(f._2.toInt),algorithm,salt)))
	val DataFrameTmp = RDDtmp.toDF("key", "value")
	RDDtmp.toDF().registerTempTable("DF"+f._2)
	listTables = ("DF"+f._2) +: listTables
	sqlTemp    += "INNER JOIN " + ("DF"+f._2) + " d"+f._2 + " ON o.xxx = " + " d"+f._2 + ".key "
	}
	val sqlDF = sqlContext.sql(sqlTemp)
	sqlDF.show()
}

// call function for obtaining Key/Values
val conf_file	    = "/home/cloudera/Desktop/spark/config.txt"
val path_hdfs	    = "/spark/data.txt"
val algorithm       = "MD5"
val salt 		    = "c4d4"
val arrayTuple      = obtainParKeyValue(conf_file, path_hdfs)
 
// load data to encript
val RDDinputFile    = sc.textFile(path_hdfs).map(x => x.split('|'))

// call function for generate dataset 
generateDataset(RDDinputFile, arrayTuple, algorithm, salt)



