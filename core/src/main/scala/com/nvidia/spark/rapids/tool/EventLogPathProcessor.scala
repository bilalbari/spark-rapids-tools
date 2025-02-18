/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.tool

import java.io.FileNotFoundException
import java.time.LocalDateTime
import java.util.zip.ZipOutputStream

import scala.collection.mutable.LinkedHashMap
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileStatus, FileSystem, Path, PathFilter}

import org.apache.spark.deploy.history.{EventLogFileReader, EventLogFileWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.util.FSUtils
import org.apache.spark.sql.rapids.tool.util.StringUtils

sealed trait EventLogInfo {
  def eventLog: Path
}

case class EventLogFileSystemInfo(timestamp: Long, size: Long)

case class ApacheSparkEventLog(override val eventLog: Path) extends EventLogInfo
case class DatabricksEventLog(override val eventLog: Path) extends EventLogInfo

case class FailedEventLog(override val eventLog: Path,
                          private val reason: String) extends EventLogInfo {
  def getReason: String = {
    StringUtils.renderStr(reason, doEscapeMetaCharacters = true, maxLength = 0)
  }
}

object EventLogPathProcessor extends Logging {
  // Apache Spark event log prefixes
  val EVENT_LOG_DIR_NAME_PREFIX = "eventlog_v2_"
  val EVENT_LOG_FILE_NAME_PREFIX = "events_"
  val DB_EVENT_LOG_FILE_NAME_PREFIX = "eventlog"

  def isEventLogDir(status: FileStatus): Boolean = {
    status.isDirectory && isEventLogDir(status.getPath.getName)
  }

  // This only checks the name of the path
  def isEventLogDir(path: String): Boolean = {
    path.startsWith(EVENT_LOG_DIR_NAME_PREFIX)
  }

  def isDBEventLogFile(fileName: String): Boolean = {
    fileName.startsWith(DB_EVENT_LOG_FILE_NAME_PREFIX)
  }

  def isDBEventLogFile(status: FileStatus): Boolean = {
    status.isFile && isDBEventLogFile(status.getPath.getName)
  }

  // https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/
  // core/src/main/scala/org/apache/spark/io/CompressionCodec.scala#L67
  val SPARK_SHORT_COMPRESSION_CODEC_NAMES = Set("lz4", "lzf", "snappy", "zstd")
  // Apache Spark ones plus gzip
  val SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER =
    SPARK_SHORT_COMPRESSION_CODEC_NAMES ++ Set("gz")

  // Show a special message if the eventlog is one of the following formats
  // https://github.com/NVIDIA/spark-rapids-tools/issues/506
  val EVENTLOGS_IN_PLAIN_TEXT_CODEC_NAMES = Set("log", "txt")

  def eventLogNameFilter(logFile: Path): Boolean = {
    EventLogFileWriter.codecName(logFile)
      .forall(suffix => SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER.contains(suffix))
  }

  def isPlainTxtFileName(logFile: Path): Boolean = {
    EventLogFileWriter.codecName(logFile)
      .forall(suffix => EVENTLOGS_IN_PLAIN_TEXT_CODEC_NAMES.contains(suffix))
  }

  // Databricks has the latest events in file named eventlog and then any rolled in format
  // eventlog-2021-06-14--20-00.gz, here we assume that if any files start with eventlog
  // then the directory is a Databricks event log directory.
  def isDatabricksEventLogDir(dir: FileStatus, fs: FileSystem): Boolean = {
    val dbLogFiles = fs.listStatus(dir.getPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        isDBEventLogFile(path.getName)
      }
    })
    (dbLogFiles.size > 1)
  }

  /**
   * If the user provides wildcard in the eventlogs, then the path processor needs
   * to process the path as a pattern. Otherwise, HDFS throws an exception mistakenly that
   * no such file exists.
   * Once the glob path is checked, the list of eventlogs is the result of the flattenMap
   * of all the processed files.
   *
   * @return List of (rawPath, List[processedPaths])
   */
  private def processWildcardsLogs(eventLogsPaths: List[String],
      hadoopConf: Configuration): List[(String, List[String])] = {
    eventLogsPaths.map { rawPath =>
      if (!rawPath.contains("*")) {
        (rawPath, List(rawPath))
      } else {
        try {
          val globPath = new Path(rawPath)
          val fileContext = FileContext.getFileContext(globPath.toUri(), hadoopConf)
          val fileStatuses = fileContext.util().globStatus(globPath)
          val paths = fileStatuses.map(_.getPath.toString).toList
          (rawPath, paths)
        } catch {
          case _ : Throwable =>
            // Do not fail in this block.
            // Instead, ignore the error and add the file as is; then the caller should fail when
            // processing the file.
            // This will make handling errors more consistent during the processing of the analysis
            logWarning(s"Processing pathLog with wildCard has failed: $rawPath")
            (rawPath, List.empty)
        }
      }
    }
  }

  def getEventLogInfo(pathString: String,
      hadoopConf: Configuration): Map[EventLogInfo, Option[EventLogFileSystemInfo]] = {
    val inputPath = new Path(pathString)
    try {
      // Note that some cloud storage APIs may throw FileNotFoundException when the pathPrefix
      // (i.e., bucketName) is not visible to the current configuration instance.
      val fs = FSUtils.getFSForPath(inputPath, hadoopConf)
      val fileStatus = fs.getFileStatus(inputPath)
      val filePath = fileStatus.getPath()
      val fileName = filePath.getName()

      if (fileStatus.isFile() && !eventLogNameFilter(filePath)) {
        val msg = if (isPlainTxtFileName(filePath)) {
          // if the file is plain text, we want to show that the filePath without extension
          // could be supported..
          s"File: $fileName. Detected a text file. No extension is expected. skipping this file."
        } else {
          s"File: $fileName is not a supported file type. " +
            "Supported compression types are: " +
            s"${SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER.mkString(", ")}. " +
            "Skipping this file."
        }
        logWarning(msg)
        // Return an empty map as this is a skip due to unsupported file type, not an exception.
        // Returning FailedEventLog would clutter the status report with unnecessary entries.
        Map.empty[EventLogInfo, Option[EventLogFileSystemInfo]]
      } else if (fileStatus.isDirectory && isEventLogDir(fileStatus)) {
        // either event logDir v2 directory or regular event log
        val info = ApacheSparkEventLog(fileStatus.getPath).asInstanceOf[EventLogInfo]
        // TODO - need to handle size of files in directory, for now document its not supported
        Map(info ->
          Some(EventLogFileSystemInfo(fileStatus.getModificationTime, fileStatus.getLen)))
      } else if (fileStatus.isDirectory &&
        isDatabricksEventLogDir(fileStatus, fs)) {
        val dbinfo = DatabricksEventLog(fileStatus.getPath).asInstanceOf[EventLogInfo]
        // TODO - need to handle size of files in directory, for now document its not supported
        Map(dbinfo ->
          Some(EventLogFileSystemInfo(fileStatus.getModificationTime, fileStatus.getLen)))
      } else {
        // assume either single event log or directory with event logs in it, we don't
        // support nested dirs, so if event log dir within another one we skip it
        val (validLogs, invalidLogs) = fs.listStatus(inputPath).partition(s => {
            val name = s.getPath().getName()
            (s.isFile ||
              (s.isDirectory && (isEventLogDir(name) || isDatabricksEventLogDir(s, fs))))
          })
        if (invalidLogs.nonEmpty) {
          logWarning("Skipping the following directories: " +
            s"${invalidLogs.map(_.getPath().getName()).mkString(", ")}")
        }
        val (logsSupported, unsupport) = validLogs.partition { l =>
          (l.isFile && eventLogNameFilter(l.getPath())) || l.isDirectory
        }
        if (unsupport.nonEmpty) {
          logWarning(s"Files: ${unsupport.map(_.getPath.getName).mkString(", ")} " +
            s"have unsupported file types. Supported compression types are: " +
            s"${SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER.mkString(", ")}. " +
            "Skipping these files.")
        }
        logsSupported.map { s =>
          if (s.isFile || (s.isDirectory && isEventLogDir(s.getPath().getName()))) {
            (ApacheSparkEventLog(s.getPath).asInstanceOf[EventLogInfo]
              -> Some(EventLogFileSystemInfo(s.getModificationTime, s.getLen)))
          } else {
            (DatabricksEventLog(s.getPath).asInstanceOf[EventLogInfo]
              -> Some(EventLogFileSystemInfo(s.getModificationTime, s.getLen)))
          }
        }.toMap
      }
    } catch {
      case fnfEx: FileNotFoundException =>
        logWarning(s"$pathString not found, skipping!")
        Map(FailedEventLog(new Path(pathString), fnfEx.getMessage) -> None)
      case NonFatal(e) =>
        logWarning(s"Unexpected exception occurred reading $pathString, skipping!", e)
        Map(FailedEventLog(new Path(pathString), e.getMessage) -> None)
    }
  }

  /**
   * Function to process event log paths passed in and evaluate which ones are really event
   * logs and filter based on user options.
   *
   * @param filterNLogs    number of event logs to be selected
   * @param matchlogs      keyword to match file names in the directory
   * @param eventLogsPaths Array of event log paths
   * @param hadoopConf     Hadoop Configuration
   * @return (Seq[EventLogInfo], Seq[EventLogInfo]) - Tuple indicating paths of event logs in
   *         filesystem. First element contains paths of event logs after applying filters and
   *         second element contains paths of all event logs.
   */
  def processAllPaths(
      filterNLogs: Option[String],
      matchlogs: Option[String],
      eventLogsPaths: List[String],
      hadoopConf: Configuration,
      maxEventLogSize: Option[String] = None,
      minEventLogSize: Option[String] = None): (Seq[EventLogInfo], Seq[EventLogInfo]) = {
    val logsPathNoWildCards = processWildcardsLogs(eventLogsPaths, hadoopConf)
    val logsWithTimestamp = logsPathNoWildCards.flatMap {
      case (rawPath, processedPaths) if processedPaths.isEmpty =>
        // If no event logs are found in the path after wildcard expansion, return a failed event
        Map(FailedEventLog(new Path(rawPath), s"No event logs found in $rawPath") -> None)
      case (_, processedPaths) =>
        processedPaths.flatMap(getEventLogInfo(_, hadoopConf))
    }.toMap

    logDebug("Paths after stringToPath: " + logsWithTimestamp)
    // Filter the event logs to be processed based on the criteria. If it is not provided in the
    // command line, then return all the event logs processed above.
    val matchedLogs = matchlogs.map { strMatch =>
      logsWithTimestamp.filterKeys(_.eventLog.getName.contains(strMatch))
    }.getOrElse(logsWithTimestamp)

    val filteredLogs = if ((filterNLogs.nonEmpty && !filterByAppCriteria(filterNLogs)) ||
      maxEventLogSize.isDefined || minEventLogSize.isDefined) {
      val validMatchedLogs = matchedLogs.collect {
        case (info, Some(ts)) => info -> ts
      }
      val filteredByMinSize = if (minEventLogSize.isDefined) {
        val minSizeInBytes = if (StringUtils.isMemorySize(minEventLogSize.get)) {
          // if it is memory return the bytes unit
          StringUtils.convertMemorySizeToBytes(minEventLogSize.get)
        } else {
          // size is assumed to be mb
          StringUtils.convertMemorySizeToBytes(minEventLogSize.get + "m")
        }
        val (matched, filtered) = validMatchedLogs.partition(info => info._2.size >= minSizeInBytes)
        logInfo(s"Filtering eventlogs by size, minimum size is ${minSizeInBytes}b. The logs " +
          s"filtered out include: ${filtered.keys.map(_.eventLog.toString).mkString(",")}")
        matched
      } else {
        validMatchedLogs
      }
      val filteredByMaxSize = if (maxEventLogSize.isDefined) {
        val maxSizeInBytes = if (StringUtils.isMemorySize(maxEventLogSize.get)) {
          // if it is memory return the bytes unit
          StringUtils.convertMemorySizeToBytes(maxEventLogSize.get)
        } else {
          // size is assumed to be mb
          StringUtils.convertMemorySizeToBytes(maxEventLogSize.get + "m")
        }
        val (matched, filtered) =
          filteredByMinSize.partition(info => info._2.size <= maxSizeInBytes)
        logInfo(s"Filtering eventlogs by size, max size is ${maxSizeInBytes}b. The logs filtered " +
          s"out include: ${filtered.keys.map(_.eventLog.toString).mkString(",")}")
        matched
      } else {
        filteredByMinSize
      }
      if (filterNLogs.nonEmpty && !filterByAppCriteria(filterNLogs)) {
        val filteredInfo = filterNLogs.get.split("-")
        val numberofEventLogs = filteredInfo(0).toInt
        val criteria = filteredInfo(1)
        // Before filtering based on user criteria, remove the failed event logs
        // (i.e. logs without timestamp) from the list.
        val matched = if (criteria.equals("newest")) {
          LinkedHashMap(filteredByMaxSize.toSeq.sortWith(_._2.timestamp > _._2.timestamp): _*)
        } else if (criteria.equals("oldest")) {
          LinkedHashMap(filteredByMaxSize.toSeq.sortWith(_._2.timestamp < _._2.timestamp): _*)
        } else {
          logError("Criteria should be either newest-filesystem or oldest-filesystem")
          Map.empty[EventLogInfo, Long]
        }
        matched.take(numberofEventLogs)
      } else {
        filteredByMaxSize
      }
    } else {
      matchedLogs
    }
    (filteredLogs.keys.toSeq, logsWithTimestamp.keys.toSeq)
  }

  def filterByAppCriteria(filterNLogs: Option[String]): Boolean = {
    filterNLogs.get.endsWith("-oldest") || filterNLogs.get.endsWith("-newest") ||
        filterNLogs.get.endsWith("per-app-name")
  }

  def logApplicationInfo(app: ApplicationInfo) = {
    logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
  }

  def getDBEventLogFileDate(eventLogFileName: String): LocalDateTime = {
    if (!isDBEventLogFile(eventLogFileName)) {
      logError(s"$eventLogFileName Not an event log file!")
    }
    val fileParts = eventLogFileName.split("--")
    if (fileParts.size < 2) {
      // assume this is the current log and we want that one to be read last
      LocalDateTime.now()
    } else {
      val date = fileParts(0).split("-")
      val day = Integer.parseInt(date(3))
      val month = Integer.parseInt(date(2))
      val year = Integer.parseInt(date(1))
      val time = fileParts(1).split("-")
      val minParse = time(1).split('.')
      val hour = Integer.parseInt(time(0))
      val min = Integer.parseInt(minParse(0))
      LocalDateTime.of(year, month, day, hour, min)
    }
  }
}

/**
 * The reader which will read the information of Databricks rolled multiple event log files.
 */
class DatabricksRollingEventLogFilesFileReader(
    fs: FileSystem,
    path: Path) extends EventLogFileReader(fs, path) with Logging {

  private lazy val files: Seq[FileStatus] = {
    val ret = fs.listStatus(rootPath).toSeq
    if (!ret.exists(EventLogPathProcessor.isDBEventLogFile)) {
      Seq.empty[FileStatus]
    } else {
      ret
    }
  }

  private lazy val eventLogFiles: Seq[FileStatus] = {
    files.filter(EventLogPathProcessor.isDBEventLogFile).sortWith { (status1, status2) =>
      val dateTime = EventLogPathProcessor.getDBEventLogFileDate(status1.getPath.getName)
      val dateTime2 = EventLogPathProcessor.getDBEventLogFileDate(status2.getPath.getName)
      dateTime.isBefore(dateTime2)
    }
  }

  override def completed: Boolean = true
  override def modificationTime: Long = lastEventLogFile.getModificationTime
  private def lastEventLogFile: FileStatus = eventLogFiles.last
  override def listEventLogFiles: Seq[FileStatus] = eventLogFiles

  // unused functions
  override def compressionCodec: Option[String] = None
  override def totalSize: Long = 0
  override def zipEventLogFiles(zipStream: ZipOutputStream): Unit = {}
  override def fileSizeForLastIndexForDFS: Option[Long] = None
  override def fileSizeForLastIndex: Long = lastEventLogFile.getLen
  override def lastIndex: Option[Long] = None
}
