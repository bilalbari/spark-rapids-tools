package org.apache.spark.benchmarker

import org.rogach.scallop.{ScallopConf, ScallopOption}

class BenchmarkArgs(arguments: Seq[String]) extends ScallopConf(arguments) {

  banner("""
Benchmarker class for running various benchmarks.

Usage: java -cp
    """)

  val iterations: ScallopOption[Int] = opt[Int](name = "iterations", short = 'i', default = Some(5),
    descr = "Total iterations to run")
  val warmupIterations: ScallopOption[Int] = opt[Int](name = "warmupIterations", short = 'w' ,
    default = Some(3), descr = "Number of warmup iterations to run")
  val outputFormat: ScallopOption[String] = opt[String](name = "outputFormat", short = 'o',
    default = Some("json"), descr = "Format of output ( tbl, json)")
  val extraArgs: ScallopOption[String] = opt[String](name = "extraArgs" , short = 'a',
    required = false,
    descr = "Extra arguments to pass to the benchmark")

  verify()
}