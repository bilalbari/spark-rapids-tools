package com.nvidia.spark.rapids.tool

import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, Scope, State}
import org.openjdk.jmh.runner.options.{Options, OptionsBuilder}

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.All))
class QualificationBenchmark {

  @Benchmark
  def benchmarkQualification(): Unit = {
    val args = Array("--output-directory",
      "/home/sbari/project-repos/scratch_folder/issue-367/output_folder",
      "--per-sql",
      "/home/sbari/project-repos/scratch_folder/issue-978/eventlogs/temp-event-logs/databricks")
    val obj = Class.forName("com.nvidia.spark.rapids.tool.qualification.QualificationMain" + "$")
      .getField("MODULE$").get(null)
    val method = obj.getClass.getMethod("main", classOf[Array[String]])
    method.invoke(obj, args)
  }
}

