//hdfs_laszip.scala
package com.utils.laszip

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}
import org.apache.hadoop.conf.Configuration
import java.nio.file.Files
import sys.process._

class LasZip(bin_path : String){

  //use to read and write from/to hadoop
  val hdfs = FileSystem.get(new Configuration())


  def decompressFolder(folderpath:String){
    val dir = Files.createTempDirectory("laz")
    hdfs.copyToLocalFile(false,
                         new Path(folderpath + "*.*"),
                         new Path(dir.toString()))

    val files = dir + "/*.*"
    this.decompress(files, "$HOME/mega.las")
  }


  private[this] def decompress(in:String, out:String){
    Seq(bin_path, in, "-o", out).!!
  }

  private[this] def decompressM(in:Seq[String], out:String){
    val cmd = (bin_path +: in) ++ Seq("-o", out)
    cmd.!!
  }

  def decompressFile(filepath : String, outfile_path : String){
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
    // println("decompress time: " + (t1 - t0)/1000000 + "ms")
    outfile_path
  }


  def compress(filepath : String, outfile_path : String){
    //TODO
  }
}
