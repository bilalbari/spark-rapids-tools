package org.apache.spark.sql.rapids.tool.store

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.sql.rapids.tool.kvstore.KVLocalStore

class TaskModelManagerDisk extends TaskModelManagerTrait with Logging {

  private val stageAttemptToTasks:
    mutable.SortedMap[Int, mutable.SortedMap[Int, ArrayBuffer[(Int, Int, Long, Int)]]] =
    mutable.SortedMap[Int, mutable.SortedMap[Int, ArrayBuffer[(Int,Int, Long, Int)]]]()

  private val kvStore = getLocalStore

  private def getLocalStore:KVLocalStore = {
    try
    {
      new KVLocalStore("TaskModelManagerDisk")
    } catch {
      case e: Exception =>
        throw new Exception("Error in getting local store", e)
    }
  }

  // Given a Spark taskEnd event, create a new Task and add it to the Map.
  def addTaskFromEvent(event: SparkListenerTaskEnd): Unit = {
    // Creating taskModel Object
    logInfo("Adding task from event")
    synchronized {
      val taskModel = TaskModel(event)
      // Writing data to disk
      kvStore.write(taskModel)

      logInfo("Task written to disk with id: " + taskModel.id)
      // Adding taskModel id to in-memory map to refer later
      val stageAttempts =
        stageAttemptToTasks.getOrElseUpdate(event.stageId,
          mutable.SortedMap[Int, ArrayBuffer[(Int, Int, Long, Int)]]())
      logInfo("Stage attempts updated")
      val attemptToTasks =
        stageAttempts.getOrElseUpdate(event.stageAttemptId, ArrayBuffer[(Int, Int, Long, Int)]())
      attemptToTasks += taskModel.id
      logInfo("Task added to attempt")
    }
  }

  // Given a stageID and stageAttemptID, return all tasks or Empty iterable.
  // The returned tasks will be filtered by the the predicate func if the latter exists
  def getTasks(stageID: Int, stageAttemptID: Int,
      predicateFunc: Option[TaskModel => Boolean] = None): Iterable[TaskModel] = {
    logInfo("Getting tasks")
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
    logInfo("Getting all tasks for stage attempt")
    stageAttemptToTasks.get(stageID).map { stageAttempts =>
      stageAttempts.values.flatten.map(kvStore.read(classOf[TaskModel],_))
    }.getOrElse(Iterable.empty)
  }

  // Returns the combined list of all tasks that satisfy the predicate function if it exists.
  def getAllTasks(predicateFunc: Option[TaskModel => Boolean] = None): Iterable[TaskModel] = {
    logInfo("Getting all tasks")
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

  def getAllTasksIndexedByStageAttempt: Map[(Int, Int) , Seq[TaskModel]] = {
    var results = Map[(Int, Int), Seq[TaskModel]]()
    logInfo("Getting all tasks indexed by stage attempt")
    stageAttemptToTasks.foreach { case (stageId, stageAttempts) =>
      stageAttempts.foreach { case (stageAttemptId, tasks) =>
        val tempBuffer = ArrayBuffer[TaskModel]()
        tasks.foreach(taskId => {
          tempBuffer.append(kvStore.read(classOf[TaskModel],taskId))
        })
        results += (stageId, stageAttemptId) -> tempBuffer
      }
    }
    results
  }

  def getAllTasksIndexedByStage: Map[Int, Seq[TaskModel]] = {
    logInfo("Getting all tasks indexed by stage")
    var results = Map[Int, Seq[TaskModel]]()
    stageAttemptToTasks.foreach { case (stageId, stageAttempts) =>
      val tempBuffer = ArrayBuffer[TaskModel]()
      stageAttempts.foreach { case (_, tasks) =>
        tasks.foreach(taskId => {
          tempBuffer.append(kvStore.read(classOf[TaskModel],taskId))
        })
      }
      results += stageId -> tempBuffer
    }
    results
  }
}
