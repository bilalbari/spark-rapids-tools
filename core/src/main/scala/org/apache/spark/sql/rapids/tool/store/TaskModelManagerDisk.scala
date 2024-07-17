package org.apache.spark.sql.rapids.tool.store

import scala.collection.mutable.{ArrayBuffer, SortedMap}
import scala.collection.mutable

import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.sql.rapids.tool.kvstore.KVLocalStore

class TaskModelManagerDisk extends TaskModelManagerTrait {

  private val stageAttemptToTasks:
    mutable.SortedMap[Int, mutable.SortedMap[Int, ArrayBuffer[(Int, Int, Long)]]] =
    mutable.SortedMap[Int, mutable.SortedMap[Int, ArrayBuffer[(Int,Int, Long)]]]()

  private val kvStore = new KVLocalStore("TaskModelManagerDisk")

  // Given a Spark taskEnd event, create a new Task and add it to the Map.
  def addTaskFromEvent(event: SparkListenerTaskEnd): Unit = {
    // Creating taskModel Object
    val taskModel = TaskModel(event)
    // Writing data to disk
    kvStore.write(taskModel)
    // Adding taskModel id to in-memory map to refer later
    val stageAttempts =
      stageAttemptToTasks.getOrElseUpdate(event.stageId,
        mutable.SortedMap[Int, ArrayBuffer[(Int, Int, Long)]]())
    val attemptToTasks =
      stageAttempts.getOrElseUpdate(event.stageAttemptId, ArrayBuffer[(Int, Int, Long)]())
    attemptToTasks += taskModel.id
  }

  // Given a stageID and stageAttemptID, return all tasks or Empty iterable.
  // The returned tasks will be filtered by the the predicate func if the latter exists
  def getTasks(stageID: Int, stageAttemptID: Int,
      predicateFunc: Option[TaskModel => Boolean] = None): Iterable[TaskModel] = {
    stageAttemptToTasks.get(stageID).flatMap { stageAttempts =>
      stageAttempts.get(stageAttemptID).map { tasks =>
        if (predicateFunc.isDefined) {
          tasks.map(kvStore.read(classOf[TaskModel],_)).filter(predicateFunc.get)
        } else {
          tasks.map(kvStore.read(classOf[TaskModel],_))
        }
      }
    }.getOrElse(Iterable.empty)
  }

  // Returns the combined list of tasks that belong to a specific stageID.
  // This includes tasks belonging to different stageAttempts.
  // This is mainly supporting callers that use stageID (without attemptID).
  def getAllTasksStageAttempt(stageID: Int): Iterable[TaskModel] = {
    stageAttemptToTasks.get(stageID).map { stageAttempts =>
      stageAttempts.values.flatten.map(kvStore.read(classOf[TaskModel],_))
    }.getOrElse(Iterable.empty)
  }

  // Returns the combined list of all tasks that satisfy the predicate function if it exists.
  def getAllTasks(predicateFunc: Option[TaskModel => Boolean] = None): Iterable[TaskModel] = {
    stageAttemptToTasks.collect {
      case (_, attemptsToTasks) if attemptsToTasks.nonEmpty =>
        if (predicateFunc.isDefined) {
          attemptsToTasks.values.flatten.map(kvStore.read(classOf[TaskModel],_)).
            filter(predicateFunc.get)
        } else {
          attemptsToTasks.values.flatten.map(kvStore.read(classOf[TaskModel],_))
        }
    }.flatten
  }

  // Return a list of tasks that failed within all the stageAttempts
  def getAllFailedTasks: Iterable[TaskModel] = {
    getAllTasks(Some(!_.successful))
  }

  // Given an iterable of StageIDs, return all the tasks that belong to these stages. Note that
  // this include tasks within multiple stageAttempts.
  // This is implemented to support callers that do not use stageAttemptID in their logic.
  def getTasksByStageIds(stageIds: Iterable[Int]): Iterable[TaskModel] = {
    stageIds.flatMap(getAllTasksStageAttempt)
  }
}
