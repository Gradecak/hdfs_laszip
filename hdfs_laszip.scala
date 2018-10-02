//hdfs_laszip.scala
package com.utils.laszip

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}
import org.apache.hadoop.conf.Configuration
import sys.process._

class LasZip(bin_path : String){

  //use to read and write from/to hadoop
  val hdfs = FileSystem.get(new Configuration())



  def decompress(filepath : String, outfile_path : String){
    val t0 = System.nanoTime()
    hdfs.copyToLocalFile(false,
                         new Path(filepath),
                         new Path(outfile_path))
    val res = Seq(bin_path, outfile_path, "-o", "decompressed.las").!!
    if(res != ""){
      println(res)
    }
    //remove file from local after copying to HDFS
    hdfs.copyFromLocalFile(true,
                       new Path("./decompressed.las"),
                           new Path(outfile_path))
    val t1 = System.nanoTime()
    println("decompress time: " + (t1 - t0)/1000000 + "ms")
    //TODO decompress file, return decomressed file path
  }


  def compress(filepath : String, outfile_path : String){
    //TODO
  }
}
