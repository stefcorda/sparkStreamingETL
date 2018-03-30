package ingestion.util

import java.util.Date
import java.text.SimpleDateFormat

object DateFormatter {

  def formattedDate(implicit dateFormat: SimpleDateFormat): String = dateFormat.format(new Date())
  def formattedDate(date: Date)(implicit dateFormat: SimpleDateFormat): String = dateFormat.format(date)


}
