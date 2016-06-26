package org.littlewings.spark

//import org.apache.lucene.analysis.ja.JapaneseAnalyzer
import org.codelibs.neologd.ipadic.lucene.analysis.ja.JapaneseAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import java.util.Properties
import java.sql.{Connection, DriverManager, ResultSet}


//import org.atilika.kuromoji.{Token, Tokenizer}

object TwitterStreaming {

  def main(args: Array[String]): Unit = {

    println("\n====================== Start. ======================")
    // Set Twitter Access Keys
    //    logger.debug(s"Some message!")
    val config = new java.util.Properties
    // todo: ipかなにかで  devとprod用で設定ファイルを切り替える
    config.load(this.getClass().getClassLoader().getResourceAsStream("config.properties"))
    System.setProperty("twitter4j.oauth.consumerKey", config.get("oauth.consumerKey").toString)
    System.setProperty("twitter4j.oauth.consumerSecret", config.get("oauth.consumerSecret").toString)
    System.setProperty("twitter4j.oauth.accessToken", config.get("oauth.accessToken").toString)
    System.setProperty("twitter4j.oauth.accessTokenSecret", config.get("oauth.accessTokenSecret").toString)

    println(config.get("oauth.accessTokenSecret").toString)

    //sparkconf 設定
    val conf = new SparkConf().setAppName("Twitter Streaming")
    // sparkstreamcontext設定
    val minuteunit: Long = if (args(0).isEmpty) 5 else args(0).toLong
    val ssc = new StreamingContext(conf, Durations.minutes(minuteunit))
    val filter = if (args(1).isEmpty) Nil else Array(args(1)).toList

    val stream = TwitterUtils.createStream(ssc, None, filter)

    //    println(stream)
    //    System.exit(0)

    val streamtemp = stream
      .flatMap { status =>
        val text = status.getText.replaceAll("http(s*)://(.*)/", "").replaceAll("¥¥uff57", "").replaceAll(args(1).toString, "")
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
        }
      }

    // (Apache, 1) (Spark, 1) というペアにします。２桁以上の文字を対象にアルファベット、数値のみは除外
    val streamtemp2 = streamtemp.map(word => (if (word.length >= 2) word.replaceAll("(^[a-z]+$)", "").replaceAll("^[0-9]+$", "") else "", 1))
    // countup reduceByKey(_ + _) は　reduceByKey((x, y) => x + y) と等価です。
    val streamtemp3 = streamtemp2.reduceByKey((a, b) => a + b )
      // データ保存先をconfigから取得してdevとliveで保存先切り替える
    streamtemp3.saveAsTextFiles(config.get("save.file.dir").toString)

    // streaming start
    ssc.start()
    ssc.awaitTermination()
  }

  //  def dbconnection();
  //  Unit = {
  //    val dbDriver = "com.mysql.jdbc.Driver"
  //    val dbUrl = "jdbc:mysql://localhost:3306/jdbcrdd?useUnicode=true&characterEncoding=UTF-8"
  //    val dbUsername = "hogehoge"
  //    val dbPassword = "piyopiyo"
  //
  //    val jdbcConnection = () => {
  //      Class.forName(dbDriver).newInstance
  //      DriverManager.getConnection(dbUrl, dbUsername, dbPassword)
  //    }
  //
  //  }
}
