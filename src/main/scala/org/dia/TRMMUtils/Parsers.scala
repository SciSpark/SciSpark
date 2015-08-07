package org.dia.TRMMUtils

import java.util.{Calendar, Date}
import com.joestelmach.natty.Parser
import org.apache.log4j._
object Parsers {

  def ParseDateFromString(Name: String): Date = {
    val Parser = new Parser
    val DataGroups = Parser.parse(Name)
    if (DataGroups.size() > 0) {
      val calendar = Calendar.getInstance
      calendar.setTime(DataGroups.get(0).getDates.get(0))
      calendar.getTime
    } else {
      null
    }
  }
}
