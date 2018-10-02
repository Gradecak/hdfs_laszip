//hdfs_laszip.scala
package com.utils.laszip

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}
import org.apache.hadoop.conf.Configuration
import sys.process._

class LasZip(bin_path : String){

  //use to read and write from/to hadoop
  val hdfs = FileSystem.get(new Configuration())



  def decompress(filepath : String, outfile_path : String){
    hdfs.copyToLocalFile(false,
                         new Path(filepath),
                         new Path(outfile_path))
    val res = Seq(bin_path, outfile_path, "-o", "decompresed.las").!!
      println(res)
    //TODO decompress file, return decomressed file path
  }


  def compress(filepath : String, outfile_path : String){
    //TODO
  }
}
