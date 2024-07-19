package org.apache.spark.sql.rapids.tool.kvstore

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.status.AppStatusStoreMetadata
import org.apache.spark.status.KVUtils.open
import org.apache.spark.util.kvstore.KVStore

class KVLocalStore(name: String) extends Logging{
  private val _path = File.createTempFile(name, "store")
  private val db: KVStore = openConnection

  private def openConnection: KVStore = {
    try{
      open(_path,
        AppStatusStoreMetadata(2L),
        new SparkConf().set("spark.history.store.hybridStore.diskBackend", "rocksdb"), live=true)
    }
    catch {
      case e: Exception =>
        logError(s"Error opening connection to local store at ${_path}", e)
        throw e
    }
  }

  def write[T](obj: T): Unit = {
    db.write(obj)
  }

  def read[T](cls: Class[T], key: Any): T = {
    db.read(cls, key)
  }

  def delete[T](cls: Class[T], key: Any): Unit = {
    db.delete(cls, key)
  }

}
