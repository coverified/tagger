/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging.main

import sttp.model.Uri
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

final case class Config(
    noOfConcurrentWorker: Int,
    batchSize: Int,
    graphQLApi: Uri,
    authSecret: String,
    kiApi: Uri,
    internalScheduleInterval: Long = -1
)

object Config extends LazyLogging {

  private val NO_OF_CONCURRENT_WORKER = "NO_OF_CONCURRENT_WORKER"
  private val BATCH_SIZE = "BATCH_SIZE"
  private val GRAPHQL_API_URL = "GRAPHQL_API_URL"
  private val AUTH_SECRET = "AUTH_SECRET"
  private val KI_API_URL = "KI_API_URL"
  private val INTERNAL_SCHEDULER_INTERVAL = "INTERNAL_SCHEDULER_INTERVAL"

  // all time values in milliseconds
  private val defaultParams: Map[String, String] = Map(
    NO_OF_CONCURRENT_WORKER -> 10.toString,
    BATCH_SIZE -> 10.toString,
    GRAPHQL_API_URL -> "",
    AUTH_SECRET -> "",
    KI_API_URL -> "",
    INTERNAL_SCHEDULER_INTERVAL -> "-1"
  )

  private val envParams: Map[String, String] =
    defaultParams.keys
      .zip(defaultParams.values)
      .map {
        case (envParam, defaultVal)
            if envParam.equals(NO_OF_CONCURRENT_WORKER) =>
          envParam -> sys.env
            .get(envParam)
            .flatMap(noOfConcurrentWorker => {
              if (noOfConcurrentWorker.isEmpty)
                throw new IllegalArgumentException(
                  s"$envParam cannot be empty!"
                )
              noOfConcurrentWorker.toIntOption.map(no => {
                if (noOfConcurrentWorker.toInt < 1) {
                  logger.warn(
                    s"$envParam is below 0. Fallback to default value: $defaultVal "
                  )
                  defaultVal
                } else {
                  no.toString
                }
              })
            })
            .getOrElse(defaultVal)
        case (envParam, defaultVal) =>
          envParam -> sys.env
            .getOrElse(
              envParam, {
                if (defaultVal.isEmpty)
                  throw new IllegalArgumentException(
                    s"$envParam cannot be empty!"
                  )
                logger.warn(
                  s"No env value provided for param $envParam. Fallback to default value: $defaultVal "
                )
                defaultVal
              }
            )
      }
      .toMap

  def apply(): Try[Config] = {
    Try {
      new Config(
        envParams(NO_OF_CONCURRENT_WORKER).toInt,
        envParams(BATCH_SIZE).toInt,
        Uri.unsafeParse(envParams(GRAPHQL_API_URL)),
        envParams(AUTH_SECRET),
        Uri.unsafeParse(envParams(KI_API_URL)),
        envParams(INTERNAL_SCHEDULER_INTERVAL).toLong
      )
    }
  }

}
