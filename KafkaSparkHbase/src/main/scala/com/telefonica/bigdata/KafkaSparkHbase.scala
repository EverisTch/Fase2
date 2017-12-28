package com.telefonica.bigdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object KafkaSparkHbase {

  def main(args : Array[String]) : Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkKafka")
    val ssc  = new StreamingContext(conf, Seconds(2))

    val topics      = Set("NetcattoKafka")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "kafka01-prod02.messagehub.services.us-south.bluemix.net:9093,kafka03-prod02.messagehub.services.us-south.bluemix.net:9093,kafka04-prod02.messagehub.services.us-south.bluemix.net:9093,kafka02-prod02.messagehub.services.us-south.bluemix.net:9093,kafka05-prod02.messagehub.services.us-south.bluemix.net:9093")
    val messages    = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    messages.print()
    
    messages.foreachRDD {
      rdd => 
        { 
          println(rdd.toDebugString) 
          rdd.saveAsTextFile("RDDKafka")
        }
    }
  }
  
}
