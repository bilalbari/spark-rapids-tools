package org.apache.spark.sql.rapids.tool.store

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.sql.rapids.tool.util.StringUtils

// Class for AppMeta containing the id and name of the accumulator
case class AccumMeta(
  id: Long,
  name: Option[String])

object AccumMeta {
  // Maintain a map of all incoming names to prevent repetition
  private val nameMetaMap = mutable.Set[Option[String]]()
  // Check if the name is already present in the map, if not add it
  // and create a new id
  def apply(l: Long, nameString: Option[String]): AccumMeta = {
    nameMetaMap.find(o => o == nameString).getOrElse({
      nameMetaMap.add(nameString)
      nameString
    })
    new AccumMeta(l, nameString)
  }
}

// AccumInfo class containing meta and maps for task updates and stage values
class AccumInfo {
  var meta: AccumMeta = _
  var taskUpdatesMap: mutable.HashMap[Long, Long] =
    new mutable.HashMap[Long, Long]()
  var stageValuesMap: mutable.HashMap[Int, Long] =
    new mutable.HashMap[Int, Long]()
}

object AccumInfo {

  // Maintain a set of all created AccumMeta objects
  private val accumMetaMap = new mutable.HashSet[AccumMeta]()
  // During creating an AccumInfo, we check if the incoming accumulator is already present
  // or not. In case yes we create a new AccumInfo object and update the stage
  // values and task updates
  def parseAccumFieldToLong(data: Any): Option[Long] = {
    val strData = data.toString
    try {
      Some(strData.toLong)
    } catch {
      case _ : NumberFormatException =>
        StringUtils.parseFromDurationToLongOption(strData)
      case NonFatal(_) =>
        None
    }
  }

  def apply(incomingAccum: AccumulableInfo, accumInfo: Option[AccumInfo], stageId: Int,
      taskId: Option[Long]): AccumInfo = {
    val newAccumInfo = accumInfo match {
      case Some(ai) =>
        val parsedLongValue = incomingAccum.value.flatMap(parseAccumFieldToLong)
        ai.stageValuesMap(stageId) =
          if (parsedLongValue.isDefined) {
          Math.max(ai.stageValuesMap(stageId),parsedLongValue.get)
        } else {
          0L
        }
        taskId match {
          case Some(tId) =>
            val parsedLongUpdate = incomingAccum.update.flatMap(parseAccumFieldToLong)
            ai.taskUpdatesMap(tId) =
              if (parsedLongUpdate.isDefined) {
              parsedLongUpdate.get
            } else {
              0L
            }
          case _ =>
        }
        ai
      case None =>
        val newAccumMeta = accumMetaMap.find(AccumMeta(incomingAccum.id, incomingAccum.name)
          == _)
          .getOrElse({
          val meta = AccumMeta(incomingAccum.id, incomingAccum.name)
          accumMetaMap.add(meta)
          meta
        })
        val ai = new AccumInfo
        ai.meta = newAccumMeta
        val parsedLongValue = incomingAccum.value.flatMap(parseAccumFieldToLong)
        ai.stageValuesMap(stageId) = if (parsedLongValue.isDefined) {
          parsedLongValue.get
        } else {
          0L
        }
        taskId match {
          case Some(tId) =>
            val parsedLongUpdate = incomingAccum.update.flatMap(parseAccumFieldToLong)
            ai.taskUpdatesMap(tId) = if (parsedLongUpdate.isDefined) {
              parsedLongUpdate.get
            } else {
              0L
            }
          case _ =>
        }
        ai
    }
    newAccumInfo
  }
}
