import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.io.IOUtils
import scopt.OParser

import java.net.URI
import scala.util.Using
import scala.util.matching.Regex


object main {

  case class RuntimeConfig(
                            hadoopEndpoint: String = "hdfs://localhost:9000",
                            in: String = "/stage",
                            out: String = "/ods")

  def parseCmdLine(args: Array[String]): Option[RuntimeConfig] = {
    val builder = OParser.builder[RuntimeConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("app"),
        head("app", "0.1"),
        opt[String]("he")
          .action((x, c) => c.copy(hadoopEndpoint = x))
          .optional()
          .text("Hadoop endpoint (optional)"),
        opt[String]("in")
          .action((x, c) => c.copy(in = x))
          .optional()
          .text("Input directory (optional)"),
        opt[String]("out")
          .action((x, c) => c.copy(out = x))
          .optional()
          .text("Output directory (optional)"),
      )
    }
    OParser.parse(parser, args, RuntimeConfig())
  }

  def main(args: Array[String]): Unit = {
    val a = Seq("a", "b", "c", "aa")
    val b = a.groupBy(_.charAt(0))

    val runtimeConfig = parseCmdLine(args)
    if (runtimeConfig.isEmpty) {
      return
    }

    val conf = new Configuration()
    Using.Manager { use =>
      val fileSystem = use(FileSystem.get(new URI(runtimeConfig.get.hadoopEndpoint), conf))

      val inPath = new Path(runtimeConfig.get.in)
      val outPath = new Path(runtimeConfig.get.out)

      val files = getFiles(fileSystem, inPath)

      println(files)
    }
  }

  // Traverse HDFS filesystem path and all subdirectories and return all files
  def getFiles(fileSystem: FileSystem, path: Path): Seq[Path] = {
    val status = fileSystem.listStatus(path)
    val files = status.filter(_.isFile).map(_.getPath)
    val dirs = status.filter(_.isDirectory).map(_.getPath)
    files ++ dirs.flatMap(getFiles(fileSystem, _))
  }

}