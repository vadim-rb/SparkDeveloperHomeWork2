import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path.mergePaths
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

object Main {
  def copyMerge(
                 srcFS: FileSystem, srcDir: Path,
                 dstFS: FileSystem, dstFile: Path,
                 deleteSource: Boolean, conf: Configuration
               ): Boolean = {
    if (srcFS.getFileStatus(srcDir).isDirectory) {
      val outputFile = dstFS.create(dstFile)
      try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile &
              !status.getPath.getName.startsWith(".") &
                status.getPath.getName != dstFile.getName  =>
              val inputFile = srcFS.open(status.getPath)
              try {
                IOUtils.copyBytes(inputFile, outputFile, conf, false)
                if (deleteSource) srcFS.delete(status.getPath, true)
              }
              finally {
                inputFile.close()
              }
          }
      } finally {
        outputFile.close()
      }
      true
    }
    else false
  }

  def main(args: Array[String]): Unit = {
    println("Hello hdfs!")
    val conf = new Configuration()
    val hdfsCoreSitePath = new Path("core-site.xml")
    val hdfsHDFSSitePath = new Path("hdfs-site.xml")
    conf.addResource(hdfsCoreSitePath)
    conf.addResource(hdfsHDFSSitePath)
    val srcFs = FileSystem.get(conf)
    val srcDir = new Path("/stage")
    for (folder <- srcFs.listStatus(srcDir)) {
      val output = mergePaths(folder.getPath, new Path("/part-0000.csv.merged"))
      println(f"Process folder ${folder.getPath} -> Merged file is ${output}")
      val result = copyMerge(srcFs,folder.getPath,srcFs,output,deleteSource = true, conf)
      if (result) {
        srcFs.rename(output,mergePaths(folder.getPath, new Path("/part-0000.csv")))
        println(f"Renamed file ${output} to part-0000.csv")
      } else
        println("copyMerge failed")

    }
  }
}