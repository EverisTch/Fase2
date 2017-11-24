import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

import javax.xml.bind.DatatypeConverter
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.{Connection, DriverManager, DatabaseMetaData, ResultSet}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.JdbcRDD



def digest(text: String, algorithm: String, salt: String): String = {
    
    val msg = text + salt;
    val result = MessageDigest.getInstance(algorithm).digest(msg.getBytes)
    val output = DatatypeConverter.printBase64Binary(result)

        return output
    
  }

val inputFile =sc.textFile("hdfs://nn/user/rhmaripa/spicy/input/test2.txt").map(_.split('|')) 
    textFile.first()
    
    val resultado = inputFile.map(x => digest(x(1)) + "|" + digest(x(2)))
    
    inputFile.map(x=>{
  val newCol = x(1) + x(4)
  newCol+: x
 })


