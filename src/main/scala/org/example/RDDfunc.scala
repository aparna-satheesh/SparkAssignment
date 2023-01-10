package org.example

import java.text.SimpleDateFormat
import java.util.Date
import org.example.DriverCode2.spark1

object RDDfunc extends App {

  case class LogLine(debug_level: String, timestamp: Date, download_id: Integer,
                     retrieval_stage: String, rest: String);
  val dateFormat = "yyyy-MM-dd:HH:mm:ss"
  val regex = """([^\s]+), ([^\s]+)\+00:00, ghtorrent-([^\s]+) -- ([^\s]+).rb: (.*$)""".r

  val rdd = spark1.read.
    textFile("C:/Users/AparnaMenonS/IdeaProjects/untitled5/src/main/resources/log1.txt").
    flatMap(x => x match {
      case regex(debug_level, dateTime, downloadId, retrievalStage, rest) =>
        val df = new SimpleDateFormat(dateFormat)
        new Some(LogLine(debug_level, df.parse(dateTime.replace("T", ":")), downloadId.toInt, retrievalStage, rest))
      case _ => None;
    })
}
