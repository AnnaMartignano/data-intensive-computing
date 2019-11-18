package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkAverageWord")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("./checkpoint")

    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
			"zookeeper.connect" -> "localhost:2181",
			"group.id" -> "kafka-spark-streaming",
			"zookeeper.connection.timeout.ms" -> "1000")
    val topics = Set("avg")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaConf,
      topics
    )
    
    val messagesPaired = messages.map{case (key, value) => value.split(',')} 
    val pairs = messagesPaired.map(record => (record(0), record(1).toDouble))    

    // measure the average value for each key in a stateful manner: state should be in the form State[Double]
    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
      val (sum, cnt) = state.getOption.getOrElse((0.0, 0))
            val newSum = value.getOrElse(0.0) + sum
            val newCnt = cnt + 1
            state.update((newSum, newCnt))
            (key, newSum/newCnt)
    }
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space","avg",SomeColumns("word","count"))

    ssc.start()
    ssc.awaitTermination()
  }
}
