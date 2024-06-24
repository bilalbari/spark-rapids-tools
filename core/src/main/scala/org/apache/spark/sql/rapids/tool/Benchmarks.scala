package org.apache.spark.sql.rapids.tool

import java.io.File
import java.lang.reflect.Modifier
import java.nio.file.{FileSystems, Paths}
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.Try

import com.google.common.reflect.ClassPath


object Benchmarks {
  var currentProjectRoot: Option[String] = None

  def main(args: Array[String]): Unit = {
    val isFailFast = sys.env.get(
      "SPARK_BENCHMARK_FAILFAST").map(_.toLowerCase(Locale.ROOT).trim.toBoolean).getOrElse(true)
    val numOfSplits = sys.env.get(
      "SPARK_BENCHMARK_NUM_SPLITS").map(_.toLowerCase(Locale.ROOT).trim.toInt).getOrElse(1)
    val currentSplit = sys.env.get(
      "SPARK_BENCHMARK_CUR_SPLIT").map(_.toLowerCase(Locale.ROOT).trim.toInt - 1).getOrElse(0)
    var numBenchmark = 0

    var isBenchmarkFound = false
    val benchmarkClasses = ClassPath.from(
      Thread.currentThread.getContextClassLoader
    ).getTopLevelClassesRecursive("org.apache.spark").asScala.toArray
    val matcher = FileSystems.getDefault.getPathMatcher(s"glob:${args.head}")

    benchmarkClasses.foreach { info =>
      lazy val clazz = info.load
      lazy val runBenchmark = clazz.getMethod("main", classOf[Array[String]])
      // isAssignableFrom seems not working with the reflected class from Guava's
      // getTopLevelClassesRecursive.
      require(args.length > 0, "Benchmark class to run should be specified.")
      if (
        info.getName.endsWith("Benchmark") &&
          matcher.matches(Paths.get(info.getName)) &&
          Try(runBenchmark).isSuccess && // Does this has a main method?
          !Modifier.isAbstract(clazz.getModifiers) // Is this a regular class?
      ) {
        numBenchmark += 1
        if (numBenchmark % numOfSplits == currentSplit) {
          isBenchmarkFound = true

          val targetDirOrProjDir =
            new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI)
              .getParentFile.getParentFile

          // The root path to be referred in each benchmark.
          currentProjectRoot = Some {
            if (targetDirOrProjDir.getName == "target") {
              // SBT build
              targetDirOrProjDir.getParentFile.getCanonicalPath
            } else {
              // Maven build
              targetDirOrProjDir.getCanonicalPath
            }
          }

          // scalastyle:off println
          println(s"Running ${clazz.getName}:")
          // scalastyle:on println
          // Force GC to minimize the side effect.
          System.gc()
          try {
            runBenchmark.invoke(null, args.tail.toArray)
          } catch {
            case e: Throwable if !isFailFast =>
              // scalastyle:off println
              println(s"${clazz.getName} failed with the exception below:")
              // scalastyle:on println
              e.printStackTrace()
          }
        }
      }
    }

    if (!isBenchmarkFound) throw new RuntimeException("No benchmark found to run.")
  }
}
