/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging.ai

import com.typesafe.scalalogging.LazyLogging
import info.coverified.graphql.GraphQLConnector.EntryView
import scala.util.Random

trait AiConnector {
  def queryTags(entry: EntryView): Set[String]
}

object AiConnector {

  final case class TaggerAiConnector() extends AiConnector {
    override def queryTags(entry: EntryView): Set[String] = ??? // todo JH
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

    override def queryTags(dummyEntry: EntryView): Set[String] = {

      val dummyTags: Set[String] =
        (0 until random.nextInt(dummyData.size)).map(dummyData(_)).toSet

      logger.debug(s"DummyAiConnector queried tags: $dummyTags")

      dummyTags

    }
  }

}
