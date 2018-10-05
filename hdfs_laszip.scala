//hdfs_laszip.scala
package com.utils.lastools

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}
import org.apache.hadoop.conf.Configuration
import java.nio.file.Files
import sys.process._
import java.io.File

class LasTools(bin_base_path : String){

  //use to read and write from/to hadoop
  val hdfs = FileSystem.get(new Configuration())
  val homeDir = sys.env("HOME")
  val tmpDir = homeDir + "/tmp"

  //lasTools binaries
  val laszip = bin_base_path + "/laszip"
  val lasmerge = bin_base_path + "/lasmerge"


  def mergeTile(tileFolder: String){
    decompressFolder(tileFolder)
    merge(tmpDir + "/*.las", Some(tileFolder.split('/').last + ".las"));
  }

  def decompressFolder(folderpath:String){

    new File(tmpDir).mkdirs(); //create temp directory
    hdfs.copyToLocalFile(false,
                         new Path(folderpath),
                         new Path(tmpDir))
    this.decompress(tmpDir, None)
    hdfs.copyFromLocalFile(true,
                           new Path(tmpDir),
                           new Path(folderpath))
  }

  private[this] def merge(in:String, out:Option[String]){
    out match{
      case Some(o) => Seq(lasmerge, "-i", in, "-o", o).!!
      case None    => Seq(lasmerge, "-i", in).!!
    }
  }

  private[this] def decompress(in:String, out:Option[String]){
    out match{
      case Some(o) => Seq(laszip, "-i", in, "-o", o).!!
      case None    => Seq(laszip, "-i", in).!!
    }
  }

  // def decompressFile(filepath : String, outfile_path : String){
  //   hdfs.copyToLocalFile(false,
  //                        new Path(filepath),
  //                        new Path(outfile_path))
  //   val res = Seq(bin_path, outfile_path, "-o", "decompressed.las").!!
  //   if(res != ""){
  //     println(res)
  //   }
  //   //remove file from local after copying to HDFS
  //   hdfs.copyFromLocalFile(true,
  //                          new Path("./decompressed.las"),
  //                          new Path(outfile_path))
  //   // println("decompress time: " + (t1 - t0)/1000000 + "ms")
  //   outfile_path
  // }

  def compress(filepath : String, outfile_path : String){
    //TODO
  }
}
