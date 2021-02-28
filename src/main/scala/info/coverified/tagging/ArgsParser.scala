/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging

import com.typesafe.scalalogging.LazyLogging
import scopt.{OptionParser => scoptOptionParser}

/**
 * //ToDo: Class Description
 *
 * @version 0.1
 * @since 26.02.21
 */
object ArgsParser extends LazyLogging {

  final case class Args(
                         apiUrl: Option[String] = None,
                         taggerServiceApiUrl: Option[String] = None,
                         taggerServiceAuthKey: Option[String] = None
                       )

  private def buildParser: scoptOptionParser[Args] = {
    new scoptOptionParser[Args]("CoVerifiedTagger") {
      opt[String]("apiUrl")
        .action((value, args) => {
          args.copy(
            apiUrl = Option(value)
          )
        })
        .validate(
          value =>
            if (value.trim.isEmpty) failure("apiUrl cannot be empty!")
            else success
        )
        .text("Backend API Url")
        .minOccurs(1)
      opt[String]("taggerServiceApiUrl")
        .action((value, args) => {
          args.copy(
            taggerServiceApiUrl = Option(value)
          )
        })
        .validate(
          value =>
            if (value.trim.isEmpty) failure("taggerServiceApiUrl cannot be empty!")
            else success
        )
        .text("Tagger Service API Url")
        .minOccurs(1)

      opt[String]("taggerServiceAuthKey")
        .action((value, args) => {
          args.copy(
            taggerServiceAuthKey = Option(value)
          )
        })
        .validate(
          value =>
            if (value.trim.isEmpty) failure("taggerServiceAuthKey cannot be empty!")
            else success
        )
        .text("Tagger Service API Auth Key")
        .minOccurs(1)
    }
  }

  def parse(args: Array[String]): Option[Args] =
    buildParser.parse(args, init = Args())

}
