package ingestion.listeners

import java.io.{BufferedWriter, File, FileWriter}
import java.util.Date

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.StreamingQueryListener


class FileListener extends StreamingQueryListener{

  private[this] val conf = ConfigFactory.load
  private val filename = conf.getString("listeners.file.filename")

  private val file = new File(filename)
  private val bw = new BufferedWriter(new FileWriter(file))

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    bw.write(s"started query at time ${new Date()}")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    bw.write(event.progress.prettyJson)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    bw.write(
      event
        .exception
        .fold(s"query for terminated with success")((ex: String) => s"query terminated with exception: $ex")
    )
    bw.close()
  }
}
