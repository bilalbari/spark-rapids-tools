package org.apache.spark.sql.rapids.tool.store

import scala.collection.mutable.{ArrayBuffer, SortedMap}

import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.sql.rapids.tool.annotation.Since

@Since("24.04.1")
class TaskModelManagerInMemory extends TaskModelManagerTrait {

  private val stageAttemptToTasks: SortedMap[Int, SortedMap[Int, ArrayBuffer[TaskModel]]] =
    SortedMap[Int, SortedMap[Int, ArrayBuffer[TaskModel]]]()

  // Given a Spark taskEnd event, create a new Task and add it to the Map.
  def addTaskFromEvent(event: SparkListenerTaskEnd): Unit = {
    val taskModel = TaskModel(event)
    val stageAttempts =
      stageAttemptToTasks.getOrElseUpdate(event.stageId, SortedMap[Int, ArrayBuffer[TaskModel]]())
    val attemptToTasks =
      stageAttempts.getOrElseUpdate(event.stageAttemptId, ArrayBuffer[TaskModel]())
    attemptToTasks += taskModel
  }

  // Given a stageID and stageAttemptID, return all tasks or Empty iterable.
  // The returned tasks will be filtered by the the predicate func if the latter exists
  def getTasks(stageID: Int, stageAttemptID: Int,
      predicateFunc: Option[TaskModel => Boolean] = None): Iterable[TaskModel] = {
    stageAttemptToTasks.get(stageID).flatMap { stageAttempts =>
      stageAttempts.get(stageAttemptID).map { tasks =>
        if (predicateFunc.isDefined) {
          tasks.filter(predicateFunc.get)
        } else {
          tasks
        }
      }
    }.getOrElse(Iterable.empty)
  }

  // Returns the combined list of tasks that belong to a specific stageID.
  // This includes tasks belonging to different stageAttempts.
  // This is mainly supporting callers that use stageID (without attemptID).
  def getAllTasksStageAttempt(stageID: Int): Iterable[TaskModel] = {
    stageAttemptToTasks.get(stageID).map { stageAttempts =>
      stageAttempts.values.flatten
    }.getOrElse(Iterable.empty)
  }

  // Returns the combined list of all tasks that satisfy the predicate function if it exists.
  def getAllTasks(predicateFunc: Option[TaskModel => Boolean] = None): Iterable[TaskModel] = {
    stageAttemptToTasks.collect {
      case (_, attemptsToTasks) if attemptsToTasks.nonEmpty =>
        if (predicateFunc.isDefined) {
          attemptsToTasks.values.flatten.filter(predicateFunc.get)
        } else {
          attemptsToTasks.values.flatten
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

  def getAllTasksIndexedByStageAttempt: Map[(Int, Int), Seq[TaskModel]] = {
    var results = Map[(Int, Int), Seq[TaskModel]]()
    stageAttemptToTasks.foreach { case (stageId, stageAttempts) =>
      stageAttempts.foreach { case (stageAttemptId, tasks) =>
        results += (stageId, stageAttemptId) -> tasks
      }
    }
    results
  }

  def getAllTasksIndexedByStage: Map[Int, Seq[TaskModel]] = {
    var results = Map[Int, Seq[TaskModel]]()
    stageAttemptToTasks.foreach { case (stageId, stageAttempts) =>
      val tempBuffer = ArrayBuffer[TaskModel]()
      stageAttempts.foreach { case (_, tasks) =>
        tempBuffer.appendAll(tasks)
      }
      results += stageId -> tempBuffer
    }
    results
  }
}
