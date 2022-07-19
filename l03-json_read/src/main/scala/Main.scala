import java.io.FileOutputStream
import java.net.URL
import scala.util.Using


object Main {

  import com.github.plokhotnyuk.jsoniter_scala.core._
  import com.github.plokhotnyuk.jsoniter_scala.macros._
  import scopt.OParser


  /**
   * Runtime configuration class
   *
   * @param in  input file URL
   * @param out local output file name
   */
  case class Config(
                     in: String = "https://raw.githubusercontent.com/mledoze/countries/master/countries.json",
                     out: String = "")

  def parseCmdLine(args: Array[String]): Option[Config] = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("app"),
        head("app", "0.1"),
        opt[String]("in")
          .action((x, c) => c.copy(in = x))
          .optional()
          .text("Input data file (optional)"),
        opt[String]("out")
          .action((x, c) => c.copy(out = x))
          .required()
          .text("Output data file (required)")
      )
    }
    OParser.parse(parser, args, Config())
  }

  def main(args: Array[String]): Unit = {
    val config = parseCmdLine(args)
    if (config.isEmpty) {
      return
    }

    // Define input data model
    case class CountryName(common: String, official: String)
    case class Country(name: CountryName, region: String, area: Double, capital: Seq[String])

    val inputFile = new URL(config.get.in)

    val countries: Seq[Country] = Using.resource(inputFile.openStream()) {
      implicit val codec: JsonValueCodec[Seq[Country]] = JsonCodecMaker.make
      in => readFromStream(in)
    }

    // Filter data and select the subset required
    val countries_africa = countries.filter(_.region == "Africa").sortWith(_.area > _.area)
    val top_countries = countries_africa.slice(0, 10)

    // Define output data model
    case class CountryInfo(name: String, capital: String, area: Double)

    // Convert input data model to the output one
    val outData = top_countries.map({
      o =>
        CountryInfo(
          o.name.official,
          o.capital.headOption.getOrElse("<Undefined>"),
          o.area
        )
    })

    // Write output data to the local file
    Using.resource(new FileOutputStream(config.get.out)) {
      implicit val codec: JsonValueCodec[Seq[CountryInfo]] = JsonCodecMaker.make
      out => writeToStream(outData, out)
    }
  }

}