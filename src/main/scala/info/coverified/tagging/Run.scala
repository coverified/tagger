/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging

import com.typesafe.scalalogging.LazyLogging
import info.coverified.tagging.ArgsParser.Args
import info.coverified.tagging.service.Watson
import zio.{App, ExitCode, URIO}
import sttp.client3.UriContext
import sttp.client3.asynchttpclient.zio.AsyncHttpClientZioBackend
import zio.console.Console

import scala.util.{Success, Try}

/**
 * //ToDo: Class Description
 *
 * @version 0.1
 * @since 26.02.21
 */
object Run extends App with LazyLogging {

  override def run(
                    args: List[String]
                  ): URIO[zio.ZEnv with Console, ExitCode] = {

    // get args
    val tagger: Tagger =
      ArgsParser
        .parse(args.toArray)
        .flatMap {
          case Args(Some(apiUrl), Some(taggerServiceApiUrl), Some(taggerServiceAuthKey)) =>
            Some(Tagger(uri"$apiUrl", Watson(taggerServiceApiUrl, taggerServiceAuthKey)))
          case _ =>
            logger.info(
              "Trying to get configuration from environment variables ... "
            )
            None
        }
        .getOrElse(
          Try((sys.env("TAGGER_API_URL"), sys.env("TAGGER_SERVICE_API_URL"), sys.env("TAGGER_SERVICE_AUTH_KEY"))) match {
            case Success((apiUrl, taggerServiceApiUrl, taggerServiceAuthKey)) =>
              Tagger(uri"$apiUrl", Watson(taggerServiceApiUrl, taggerServiceAuthKey))
            case _ =>
              logger.info(
                "Cannot get all required configuration parameters from environment variables. Exiting! "
              )
              System.exit(-1)
              throw new RuntimeException(
                "Config parameters missing!"
              )
          }
        )

    val taggerRun = for {
      untaggedEntries <- tagger.queryUntaggedEntries
      existingTags <- tagger.queryExistingTags
      (newTags, entriesWithTags) <- tagger.updateTags(
        untaggedEntries,
        existingTags
      )
      _ <- tagger.tagEntries(
        (newTags.flatten ++ existingTags)
          .flatMap(tag => tag.name.map(_ -> tag))
          .toMap,
        entriesWithTags
      )
    } yield ()

    taggerRun.provideCustomLayer(AsyncHttpClientZioBackend.layer()).exitCode
  }

}
