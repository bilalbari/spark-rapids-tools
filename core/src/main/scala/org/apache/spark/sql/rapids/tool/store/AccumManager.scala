package org.apache.spark.sql.rapids.tool.store

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo

case class AccumStats(
  var minUpdate: Long,
  var medianUpdate: Long,
  var maxUpdate: Long,
  var sumUpdate: Long)

class AccumManager extends Logging {

  private val accumIdToInfo: mutable.HashMap[Long, AccumInfo] =
    new mutable.HashMap[Long, AccumInfo]()

  def getAllAccumIds: Iterable[Long] = accumIdToInfo.keys

  def getOrCreateAccum(accum: AccumulableInfo, stageId: Int,
      taskId: Option[Long] = None): AccumInfo = {
      val existingEntry = accumIdToInfo.get(accum.id)
      val newAccum = AccumInfo(accum, existingEntry, stageId, taskId)
      accumIdToInfo.put(accum.id,newAccum)
      newAccum
  }

  def getAccumById(accumId: Long): Option[AccumInfo] = {
    accumIdToInfo.get(accumId)
  }

  def getAllAccums: Iterable[AccumInfo] = {
    accumIdToInfo.values
  }

  def removeAccumById(accumId: Long): Option[AccumInfo] = {
    accumIdToInfo.remove(accumId)
  }

  def getAccumStats(accumInfo: Option[AccumInfo]): AccumStats = {
    accumInfo.map { accumInfo =>
      val taskUpdates = accumInfo.taskUpdatesMap.values
      val sortedTaskUpdates = taskUpdates.toSeq.sorted
      val minUpdate = sortedTaskUpdates.head
      val maxUpdate = sortedTaskUpdates.last
      val medianUpdate = {
        val size = sortedTaskUpdates.size
        if (size % 2 == 0) {
          val mid = size / 2
          (sortedTaskUpdates(mid - 1) + sortedTaskUpdates(mid)) / 2
        } else {
          sortedTaskUpdates(size / 2)
        }
      }
      val sumUpdate = taskUpdates.sum
      AccumStats(minUpdate, medianUpdate, maxUpdate, sumUpdate)
    }.getOrElse(AccumStats(0, 0, 0, 0))
  }

  def getSortedTaskUpdates(accumId: Long): Seq[Long] = {
    accumIdToInfo.get(accumId).map { accumInfo =>
      accumInfo.taskUpdatesMap.values.toSeq.sorted
    }.getOrElse(Seq.empty)
  }

  def getMaxAcross(accumId: Long): Long = {
    val taskMax: Long = accumIdToInfo.get(accumId).map { accumInfo =>
      accumInfo.taskUpdatesMap.values.max
    }.getOrElse(0)
    val stageMax: Long = accumIdToInfo.get(accumId).map { accumInfo =>
      accumInfo.stageValuesMap.values.max
    }.getOrElse(0)
    Math.max(taskMax, stageMax)
  }

  def getAccumByName(name: Option[String]): Option[AccumInfo] = {
    accumIdToInfo.values.find(_.meta.name == name)
  }
}
