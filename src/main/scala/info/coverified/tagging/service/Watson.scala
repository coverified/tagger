/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging.service

import com.typesafe.scalalogging.LazyLogging
import info.coverified.tagging.service.TaggingHandler.TaggingResult
import info.coverified.tagging.service.Watson.{
  Payload,
  WatsonRequestFeatures,
  WatsonResponse,
  WatsonTaggingResult
}
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import scalaj.http.{Http, HttpOptions}
import io.circe.generic.auto._
import io.circe.parser._

import scala.util.{Failure, Success, Try}

object Watson {

  // request obj
  case class Categories(
      score: Option[Double] = None,
      label: Option[String] = None
  )

  private final case class WatsonRequestFeatures(
      categories: Categories = Categories()
  )

  private final case class Payload(
      text: String,
      features: WatsonRequestFeatures
  )

  // response obj
  case class Usage(
      text_units: Double,
      text_characters: Double,
      features: Double
  )

  case class WatsonResponse(
      usage: Usage,
      language: String,
      categories: Vector[Categories]
  )

  case class WatsonTaggingResult(
      response: WatsonResponse
  ) extends TaggingResult {
    override val tags: Vector[String] = response.categories.flatMap(_.label)
  }

}

final case class Watson(apiUrl: String, apiKey: String)
    extends TaggingHandler
    with LazyLogging {

  override def analyze(requestString: String): Option[TaggingResult] =
    Try {
      Http(apiUrl)
        .postData(buildPayload(cleanString(requestString)))
        .auth("apikey", apiKey)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(10000))
        .asString
        .body
    } match {
      case Failure(exception) =>
        logger.error(s"Exception occurred during Watson call: $exception")
        None
      case Success(value) =>
        decode[WatsonResponse](value).map(WatsonTaggingResult).toOption
    }

  private def cleanString(inputString: String) =
    inputString.replace("\"", "").replace("“", " ")

  private def buildPayload(requestString: String) =
    write(Payload(requestString, WatsonRequestFeatures()))(DefaultFormats)

}
