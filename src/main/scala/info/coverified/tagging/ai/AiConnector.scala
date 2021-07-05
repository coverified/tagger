/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging.ai

import info.coverified.graphql.GraphQLConnector.EntryView
import com.typesafe.scalalogging.LazyLogging
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import scalaj.http.{Http, HttpOptions}
import io.circe.generic.auto._
import io.circe.parser._
import io.sentry.Sentry

import scala.util.{Failure, Success}
import scala.util.{Random, Try}

trait AiConnector {
  def queryTags(entry: EntryView): Option[Set[String]]
}

object AiConnector {

  private final case class CategoryRequest(request: String)

  private final case class TagResponse(tags: Vector[String])

  final case class TaggerAiConnector(aiApiUri: String)
      extends AiConnector
      with LazyLogging {

    override def queryTags(entry: EntryView): Option[Set[String]] =
      entry.content.flatMap(
        entryContent =>
          Try {
            Http(aiApiUri)
              .postData(buildPayload(cleanString(entryContent)))
              .header("Content-Type", "application/json")
              .header("Charset", "UTF-8")
              .option(HttpOptions.readTimeout(10000))
              .asString
              .body
          } match {
            case Failure(exception) =>
              logger.error("Error during ai api call!", exception)
              Sentry.captureException(exception)
              None
            case Success(jsonString) =>
              decode[TagResponse](jsonString) match {
                case Left(error) =>
                  logger.error("Exception during ai result processing!", error)
                  Sentry.captureException(error)
                  None
                case Right(tagResponse) =>
                  Some(tagResponse.tags.toSet)
              }
          }
      )

    private def cleanString(inputString: String) =
      inputString.replace("\"", "").replace("“", " ")

    private def buildPayload(requestString: String): String =
      write(CategoryRequest(requestString))(DefaultFormats)
  }

  final case class DummyTaggerAiConnector()
      extends AiConnector
      with LazyLogging {

    private val random = new Random(1337)
    private val dummyData = List(
      "5G Netze",
      "Angewandte Innovationen: das Beispiel Gesundheit",
      "Arbeitsleben und Teilhabe",
      "Aus-, Fort- und Weiterbildung",
      "Außen-, Sicherheits- und Verteidigungspolitik",
      "Blockchain",
      "Cybersicherheit",
      "Daten",
      "Der Staat als Dienstleister",
      "Digitale Innovation für Umwelt, Klima und Ressourcen",
      "Digitale Innovation in der Außen-, Sicherheits- und Verteidigungspolitik",
      "Digitale Kompetenz",
      "Digitale Transformation in der Wirtschaft",
      "Digitalisierung der Verwaltung",
      "Entwicklungszusammenarbeit",
      "Ethik",
      "Europa",
      "Forschung",
      "Gesellschaft im digitalen Wandel",
      "gesellschaftliche Innovationen",
      "Gesellschaftliche Innovationen und Wandel der Arbeitswelt",
      "Gesundheit",
      "Gigabit-Netze",
      "Gigabitgesellschaft",
      "Grundlegende Innovationen in Wissenschaft und Technik",
      "Hochschulbildung",
      "Industrie4.0",
      "Infrastruktur der Öffentlichen Verwaltung",
      "Infrastruktur und Ausstattung",
      "Innovation und digitale Transformation",
      "Innovation und Start-ups",
      "Innovationen",
      "Innovationspolitik",
      "international",
      "internationale Sicherheitspolitik",
      "Internet der Dinge",
      "IT-Sicherheit",
      "Kompetente Gesellschaft",
      "kritische Infrastruktur",
      "Kultur und Medien",
      "Künstliche Intelligenz",
      "Mittelstand",
      "Mobilfunk",
      "Mobilfunk und 5G",
      "Mobilität",
      "Moderner Staat",
      "Nachhaltigkeit",
      "Öffentliche Verwaltung",
      "Schulische Bildung",
      "Sicherheit im Bereich der Kritischen Infrastrukturen",
      "Sicherheitspolitik",
      "Stadt und Land",
      "Start-ups",
      "Teilhabe",
      "Telematik",
      "Transformation in Hochschulbildung und Forschung",
      "Umwelt und Klima",
      "Verkehr und Mobilität",
      "Wandel der Arbeitswelt",
      "Weltweit",
      "Wissenschaft und Technik"
    )

    override def queryTags(dummyEntry: EntryView): Option[Set[String]] = {

      val dummyTags: Set[String] =
        (0 until random.nextInt(dummyData.size)).map(dummyData(_)).toSet

      logger.debug(s"DummyAiConnector queried tags: $dummyTags")

      Some(dummyTags)
    }
  }

}
