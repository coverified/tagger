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
  AITagView,
  EntryView,
  TaggerGraphQLConnector
}
import info.coverified.graphql.schema.CoVerifiedClientSchema.{
  AITagRelateToManyInput,
  AITagWhereUniqueInput,
  EntriesUpdateInput,
  EntryUpdateInput,
  EntryWhereInput,
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
      aiTags: Set[String],
      languages: Set[String]
  )

  final case class PersistEntries(
      aiTags: Set[AITagView],
      languages: Set[LanguageView],
      replyTo: ActorRef[PersistEntriesResponse]
  ) extends TaggerEvent

  final case class PersistEntriesResponse(entries: Int)

  final case object GracefulShutdown extends TaggerEvent

  final case class HandlingResult(
      entry: EntryView,
      aiTags: Set[String],
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
        logger.info(
          "Query untagged {} entries. Skipping first {} entries.",
          data.batchSize,
          skip
        )
        val entities: Vector[EntryView] =
          data.graphQL
            .queryEntries(
              skip,
              data.batchSize,
              EntryWhereInput(hasBeenTagged = Some(false))
            )
            .toVector
        logger.info(s"Found ${entities.size} untagged entries!")

        // request tags from ai
        val handlingResult: Vector[HandlingResult] =
          entities.map(
            entry => {
              val tags = data.ai.queryAiTags(entry).getOrElse(Set.empty)
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
          handlingResult.flatMap(_.aiTags).toSet,
          handlingResult.flatMap(_.language).toSet
        )

        apply(updatedData)

      case PersistEntries(aiTags, languages, replyTo) =>
        // mutate entries
        val aiTagMap = aiTags
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
          updateEntryMutation(
            handlingResult.entry,
            handlingResult.language.flatMap(languageMap.get),
            handlingResult.aiTags.flatMap(aiTagMap.get)
          )
        })
        logger.info("Going to update {} entries!", entryUpdates.size)

        data.graphQL.updateEntriesWithAiTags(entryUpdates)

        val persistingDuration = System.currentTimeMillis() - startPersisting
        logger.info(s"Update duration: $persistingDuration ms")

        replyTo ! PersistEntriesResponse(data.currentHandlingResults.size)

        apply(data.copy(currentHandlingResults = Vector.empty))

      case GracefulShutdown =>
        data.graphQL.close()
        logger.info("Tagger shutdown complete!")
        Behaviors.stopped
    }

  private def updateEntryMutation(
      entry: EntryView,
      languageId: Option[String],
      aiTagIds: Set[String]
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
        aiTags = Some(
          AITagRelateToManyInput(
            connect = Some(
              aiTagIds
                .map(
                  aiTagId =>
                    Some(
                      AITagWhereUniqueInput(
                        Some(aiTagId)
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
