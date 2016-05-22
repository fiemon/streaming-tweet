package org.littlewings.spark

import org.apache.lucene.analysis.ja.JapaneseAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import java.util.Properties

object TwitterStreaming {
  def main(args: Array[String]): Unit = {
    println("\n====================== Start. ======================")
    // Set Twitter Access Keys
//    logger.debug(s"Some message!")
    val config = new java.util.Properties
    config.load(this.getClass().getClassLoader().getResourceAsStream("twitter4j.properties"))
    System.setProperty("twitter4j.oauth.consumerKey", config.get("oauth.consumerKey").toString)
    System.setProperty("twitter4j.oauth.consumerSecret", config.get("oauth.consumerSecret").toString)
    System.setProperty("twitter4j.oauth.accessToken", config.get("oauth.accessToken").toString)
    System.setProperty("twitter4j.oauth.accessTokenSecret", config.get("oauth.accessTokenSecret").toString)

    println(config.get("oauth.accessTokenSecret").toString)

    //sparkconf 設定
    val conf = new SparkConf().setAppName("Twitter Streaming")
    // sparkstreamcontext設定
    val ssc = new StreamingContext(conf, Durations.minutes(1L))
    val filter = if (args.isEmpty) Nil else args.toList
    val stream = TwitterUtils.createStream(ssc, None, filter)
    println("\n====================== stream ======================")
    println(stream)

    stream
      .flatMap { status =>
        val text = status.getText
        val analyzer = new JapaneseAnalyzer
        val tokenStream = analyzer.tokenStream("", text)
        val charAttr = tokenStream.addAttribute(classOf[CharTermAttribute])

        //resetメソッドを呼んだ後に、incrementTokenメソッドでTokenを読み進めていく
        tokenStream.reset()

        try {
          Iterator
            .continually(tokenStream.incrementToken())
            .takeWhile(identity)
            .map(_ => charAttr.toString)
            .toVector
        } finally {
          tokenStream.end()
          tokenStream.close()
        }
      }
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
      .saveAsTextFiles("output/tweet")

    ssc.start()
    ssc.awaitTermination()
  }
}
