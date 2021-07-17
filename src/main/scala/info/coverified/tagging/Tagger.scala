/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.github.pemistahl.lingua.api.{
  Language,
  LanguageDetector,
  LanguageDetectorBuilder
}
import com.typesafe.scalalogging.LazyLogging
import info.coverified.graphql.GraphQLConnector.{
  EntryView,
  TagView,
  TaggerGraphQLConnector
}
import info.coverified.graphql.schema.CoVerifiedClientSchema.{
  EntriesUpdateInput,
  EntryUpdateInput,
  LanguageRelateToOneInput,
  LanguageWhereUniqueInput,
  TagRelateToManyInput,
  TagWhereUniqueInput
}
import info.coverified.graphql.schema.CoVerifiedClientSchema.Language.LanguageView
import info.coverified.tagging.ai.AiConnector

object Tagger extends LazyLogging {

  sealed trait TaggerEvent

  final case class HandleEntries(
      skip: Int,
      replyTo: ActorRef[HandleEntriesResponse]
  ) extends TaggerEvent

  final case class HandleEntriesResponse(
      tags: Set[String],
      languages: Set[String]
  )

  final case class PersistEntries(
      tags: Set[TagView],
      languages: Set[LanguageView],
      replyTo: ActorRef[PersistEntriesResponse]
  ) extends TaggerEvent

  final case class PersistEntriesResponse(entries: Int)

  final case object GracefulShutdown extends TaggerEvent

  final case class HandlingResult(
      entry: EntryView,
      tags: Set[String],
      language: Option[String]
  )

  final case class TaggerData(
      batchSize: Int,
      ai: AiConnector,
      graphQL: TaggerGraphQLConnector,
      currentHandlingResults: Vector[HandlingResult] = Vector.empty,
      detector: LanguageDetector = LanguageDetectorBuilder
        .fromAllLanguages()
        .build()
  )

  def apply(data: TaggerData): Behavior[TaggerEvent] =
    Behaviors.receiveMessage {
      case HandleEntries(skip, replyTo) =>
        // query entities from db
        val entities: Vector[EntryView] =
          data.graphQL.queryEntries(skip, data.batchSize).toVector

        // request tags from ai
        // ignores entities no tags can be found for!
        val handlingResult: Vector[HandlingResult] =
          entities.map(
            entry => {
              val tags = data.ai.queryTags(entry).getOrElse(Set.empty)
              val language =
                entry.content.flatMap(data.detector.detectLanguageOf(_) match {
                  case Language.UNKNOWN =>
                    None
                  case validLang =>
                    Some(validLang.getIsoCode639_1.toString)
                })
              HandlingResult(entry, tags, language)
            }
          )

        val updatedData = data.copy(currentHandlingResults = handlingResult)

        replyTo ! HandleEntriesResponse(
          handlingResult.flatMap(_.tags).toSet,
          handlingResult.flatMap(_.language).toSet
        )

        apply(updatedData)

      case PersistEntries(tags, languages, replyTo) =>
        // mutate entries
        val tagMap = tags
          .flatMap(tagView => tagView.name.map(name => name -> tagView.id))
          .toMap
        val languageMap = languages
          .flatMap(
            languageView =>
              languageView.name.map(name => name -> languageView.id)
          )
          .toMap

        val startPersisting = System.currentTimeMillis()

        val entryUpdates = data.currentHandlingResults.map(handlingResult => {
          updateEntryMutatione(
            handlingResult.entry,
            handlingResult.language.flatMap(languageMap.get),
            handlingResult.tags.flatMap(tagMap.get)
          )

        })

        data.graphQL.updateEntriesWithTags(entryUpdates)

        val persistingDuration = System.currentTimeMillis() - startPersisting
        logger.info(s"Persisting duration: $persistingDuration ms")

        replyTo ! PersistEntriesResponse(data.currentHandlingResults.size)

        apply(data.copy(currentHandlingResults = Vector.empty))

      case GracefulShutdown =>
        data.graphQL.close()
        logger.info("Tagger shutdown complete!")
        Behaviors.stopped
    }

  private def updateEntryMutatione(
      entry: EntryView,
      languageId: Option[String],
      tagIds: Set[String]
  ): EntriesUpdateInput = EntriesUpdateInput(
    entry.id,
    Some(
      EntryUpdateInput(
        hasBeenTagged = Some(true),
        language = languageId.flatMap(
          languageId =>
            Some(
              LanguageRelateToOneInput(
                connect = Some(
                  LanguageWhereUniqueInput(
                    Some(languageId)
                  )
                )
              )
            )
        ),
        tags = Some(
          TagRelateToManyInput(
            connect = Some(
              tagIds
                .map(
                  tagId =>
                    Some(
                      TagWhereUniqueInput(
                        Some(tagId)
                      )
                    )
                )
                .toList
            )
          )
        )
      )
    )
  )

}
