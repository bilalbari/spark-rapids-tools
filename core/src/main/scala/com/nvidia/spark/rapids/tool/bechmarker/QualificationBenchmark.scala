package com.nvidia.spark.rapids.tool.bechmarker

import scala.concurrent.duration.DurationInt

import com.nvidia.spark.rapids.tool.qualification.QualificationArgs
import com.nvidia.spark.rapids.tool.qualification.QualificationMain.mainInternal

import org.apache.spark.benchmarker.{Benchmark, BenchmarkBase}

object QualificationBenchmark extends BenchmarkBase{

  def test(): Unit = {
    runBenchmark("QualificationBenchmark"){
      val benchmarker = new Benchmark("QualificationBenchmark", 2,
        warmupTime = 50.seconds, output = output, outputPerIteration = true, minNumIters = 1)
      benchmarker.addCase("QualificationBenchmark") { _ =>
        val qualificationArgs = Array("--output-directory",
          "/home/sbari/project-repos/scratch_folder/issue-367/output_folder",
          "--per-sql","true",
          "/home/sbari/project-repos/scratch_folder/issue-978/eventlogs/temp-event-logs/databricks",
        )
        mainInternal(new QualificationArgs(qualificationArgs),
          printStdout = true, enablePB = true)
      }

      benchmarker.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    test()
  }
}