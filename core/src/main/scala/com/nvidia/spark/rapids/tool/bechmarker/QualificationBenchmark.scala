package com.nvidia.spark.rapids.tool.bechmarker

import com.nvidia.spark.rapids.tool.qualification.QualificationArgs
import com.nvidia.spark.rapids.tool.qualification.QualificationMain.mainInternal

import org.apache.spark.benchmarker.{Benchmark, BenchmarkBase}

object QualificationBenchmark extends BenchmarkBase{

  override def runBenchmarkSuite(iterations:Int, warmUpIterations:Int,
                                 outputFormat: String,
                                 mainArgs: Array[String]): Unit = {
    runBenchmark("QualificationBenchmark"){
      val benchmarker = new Benchmark("QualificationBenchmark",
        2,
        output = output,
        outputPerIteration = true,
        warmupIters = warmUpIterations,
        minNumIters = iterations,
        outputFormat = outputFormat)
      benchmarker.addCase("QualificationBenchmark") { _ =>
        mainInternal(new QualificationArgs(mainArgs),
          printStdout = true, enablePB = true)
      }
      benchmarker.run()
    }
  }
}