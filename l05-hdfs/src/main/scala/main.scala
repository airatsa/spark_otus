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
    val runtimeConfig = parseCmdLine(args)
    if (runtimeConfig.isEmpty) {
      return
    }

    val conf = new Configuration()
    Using.Manager { use =>
      val fileSystem = use(FileSystem.get(new URI(runtimeConfig.get.hadoopEndpoint), conf))

      val inPath = new Path(runtimeConfig.get.in)
      val outPath = new Path(runtimeConfig.get.out)
      val partitions = getPartitions(fileSystem, inPath)
      for (p <- partitions) {
        val files = getFiles(fileSystem, p)
        if (files.nonEmpty) {
          val outDir = new Path(outPath, p.getName)
          mergeFiles(fileSystem, new Path(outDir, files.head.getName), files, deleteSources = true)
        }
      }
    }
  }

  def getPartitions(fs: FileSystem, root: Path): Seq[Path] = {
    fs.listStatus(root, new RegExPathFilter("^date=\\d\\d\\d\\d-\\d\\d-\\d\\d$".r))
      .filter(s => s.isDirectory)
      .map(s => s.getPath)
  }

  def mergeFiles(fs: FileSystem, dest: Path, sources: Seq[Path], deleteSources: Boolean): Unit = {
    val delimiter = "\n".getBytes("UTF-8")
    Using.Manager { use =>
      val out = use(fs.create(dest))
      var isFirstFile = true

      for (s <- sources) {
        if (fs.getFileStatus(s).getLen != 0) {
          if (isFirstFile) {
            isFirstFile = false
          } else {
            out.write(delimiter)
          }
          Using.resource(fs.open(s)) {
            in => IOUtils.copyBytes(in, out, 1024 * 1024)
          }
        }
      } // for sources
    }
    // The source files have been copied, it is safe to delete them
    if (deleteSources) {
      for (s <- sources) {
        fs.delete(s, false)
      }
    }
  } // mergeFiles

  def getFiles(fs: FileSystem, root: Path): Seq[Path] = {
    fs.listStatus(root, new RegExPathFilter("^part-\\d\\d\\d\\d\\.csv$".r))
      .filter(s => s.isFile)
      .map(s => s.getPath)
  }

  class RegExPathFilter(val pattern: Regex) extends PathFilter {
    override def accept(path: Path): Boolean = {
      pattern.matches(path.getName)
    }
  }

}