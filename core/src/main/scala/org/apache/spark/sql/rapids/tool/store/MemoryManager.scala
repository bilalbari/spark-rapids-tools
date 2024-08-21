package org.apache.spark.sql.rapids.tool.store

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.status.KVUtils
import org.apache.spark.util.kvstore.{KVStore, KVStoreView}

class MemoryManager {

  private val sparkConf = new SparkConf()
  private var listing : KVStore = null

  def initialize(): Unit = {
    sparkConf.set("spark.history.store.hybridStore.diskBackend", "ROCKSDB")
    val storePath = new File("/tmp/storePath")
    listing = KVUtils.createKVStore(Option(storePath), live = true, sparkConf)
  }

  def write[T](obj: T): Unit = {
      listing.write(obj)
  }

  def read[T](klass: Class[T], key: Any): T = {
      listing.read(klass, key)
  }

  def delete[T](klass: Class[T], key: Any): Unit = {
      listing.delete(klass, key)
  }

  def view[T](klass: Class[T]): KVStoreView[T] = {
      listing.view(klass)
  }
}
