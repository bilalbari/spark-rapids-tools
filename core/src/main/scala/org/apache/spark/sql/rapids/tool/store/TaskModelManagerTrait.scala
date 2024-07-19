package org.apache.spark.sql.rapids.tool.store

import org.apache.spark.scheduler.SparkListenerTaskEnd

trait TaskModelManagerTrait {
  def addTaskFromEvent(event: SparkListenerTaskEnd): Unit
  def getTasks(stageID: Int, stageAttemptID: Int,
      predicateFunc: Option[TaskModel => Boolean] = None): Iterable[TaskModel]
  def getAllTasksStageAttempt(stageID: Int): Iterable[TaskModel]
  def getAllTasksIndexedByStageAttempt: Map[(Int, Int), Seq[TaskModel]]
  def getAllTasksIndexedByStage: Map[Int, Seq[TaskModel]]
  def getAllTasks(predicateFunc: Option[TaskModel => Boolean] = None): Iterable[TaskModel]
  def getAllFailedTasks: Iterable[TaskModel]
  def getTasksByStageIds(stageIds: Iterable[Int]): Iterable[TaskModel]
}
