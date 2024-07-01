package org.apache.spark.benchmarker

import java.io.{File, FileOutputStream, OutputStream}

import org.apache.spark.internal.config.Tests.IS_TESTING

/**
 * A base class for generate benchmark results to a file.
 * For JDK 21+, JDK major version number is added to the file names to distinguish the results.
 */
abstract class BenchmarkBase {
  var output: Option[OutputStream] = None
  /**
   * Main process of the whole benchmark.
   * Implementations of this method are supposed to use the wrapper method `runBenchmark`
   * for each benchmark scenario.
   */
  def runBenchmarkSuite(iterations: Int, warmUpIterations:Int,
                        outputFormat: String, mainArgs: Array[String]): Unit

  final def runBenchmark(benchmarkName: String)(func: => Any): Unit = {
    func
    output.foreach(_.write('\n'))
  }

  def main(args: Array[String]): Unit = {

    System.setProperty(IS_TESTING.key, "true")
    val conf = new BenchmarkArgs(args)
    val version = System.getProperty("java.version").split("\\D+")(0).toInt
    val jdkString = if (version > 17) s"-jdk$version" else ""
    val resultFileName =
        s"${this.getClass.getSimpleName.replace("$", "")}$jdkString$suffix-results.txt"
    val prefix = Benchmarks.currentProjectRoot.map(_ + "/").getOrElse("")
    val dir = new File(s"${prefix}benchmarks/")
    if (!dir.exists()) {
      println(s"Creating ${dir.getAbsolutePath} for benchmark results.")
      dir.mkdirs()
    }
    val file = new File(dir, resultFileName)
    if (!file.exists()) {
      file.createNewFile()
    }
    output = Some(new FileOutputStream(file))
    runBenchmarkSuite(
      conf.iterations(),
      conf.warmupIterations(),
      conf.outputFormat(),
      conf.extraArgs().split("\\s+").filter(_.nonEmpty))

    output.foreach { o =>
      if (o != null) {
        o.close()
      }
    }
    afterAll()
  }

  def suffix: String = ""

  /**
   * Any shutdown code to ensure a clean shutdown
   */
  def afterAll(): Unit = {}
}
