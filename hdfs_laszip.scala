//hdfs_laszip.scala
package LasZip

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}

class LasZip(bin_path : String){

  //use to read and write from/to hadoop
  val hdfs : FileSystem = FileSystemUtil
    .apply(spark.sparkContext.hadoopConfiguration)
    .getFileSystem(sourceFile)


  def decompress(filepath : String, outfile_path : String){
    hdfs.copyToLocalFile(true,
                         new Path(filepath),
                         new Path(outfile_path));
    //TODO
  }


  def compress(filepath : String, outfile_path : String){
    //TODO
  }
}
