package ingestion.util

import java.text.SimpleDateFormat
import java.util.Locale

object Implicits {
   implicit val dateFormatISO8601: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH)

}
