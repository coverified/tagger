/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.typesafe.scalalogging.LazyLogging
import info.coverified.graphql.GraphQLConnector.{
  EntryIdWithTagIds,
  EntryView,
  TagView,
  TaggerGraphQLConnector
}
import info.coverified.tagging.ai.AiConnector

object Tagger extends LazyLogging {

  sealed trait TaggerEvent

  final case class TagEntries(skip: Int, replyTo: ActorRef[TagEntriesResponse])
      extends TaggerEvent

  final case class TagEntriesResponse(tags: Set[String])

  final case class PersistEntries(
      tags: Set[TagView],
      replyTo: ActorRef[PersistEntriesResponse]
  ) extends TaggerEvent

  final case class PersistEntriesResponse(entries: Int)

  final case object GracefulShutdown extends TaggerEvent

  private type TaggingResult = (Set[String], EntryView)

  final case class TaggerData(
      batchSize: Int,
      ai: AiConnector,
      graphQL: TaggerGraphQLConnector,
      currentTaggingResults: Vector[TaggingResult] = Vector.empty
  )

  def apply(data: TaggerData): Behavior[TaggerEvent] =
    Behaviors.receiveMessage {
      case TagEntries(skip, replyTo) =>
        // query entities from db
        val entities: Vector[EntryView] =
          data.graphQL.queryEntries(skip, data.batchSize).toVector

        // request tags from ai
        // ignores entities no tags can be found for!
        val taggingResult: Vector[(Set[String], EntryView)] =
          entities.flatMap(
            entity => (data.ai.queryTags(entity).map(tags => (tags, entity)))
          )

        val updatedData = data.copy(currentTaggingResults = taggingResult)

        replyTo ! TagEntriesResponse(taggingResult.flatMap(_._1).toSet)

        apply(updatedData)

      case PersistEntries(tags, replyTo) =>
        // mutate entries
        val tagMap = tags
          .flatMap(tagView => tagView.name.map(name => name -> tagView.id))
          .toMap

        val startPersisting = System.currentTimeMillis()
        val tagIdsToEntry: Vector[EntryIdWithTagIds] =
          data.currentTaggingResults.map {
            case (tags, entry) => (entry.id, tags.flatMap(tagMap.get))
          }
        data.graphQL.updateEntriesWithTags(tagIdsToEntry)

        val persistingDuration = System.currentTimeMillis() - startPersisting
        logger.info(s"Persisting duration: $persistingDuration ms")

        replyTo ! PersistEntriesResponse(data.currentTaggingResults.size)

        apply(data.copy(currentTaggingResults = Vector.empty))

      case GracefulShutdown =>
        data.graphQL.close()
        logger.info("Tagger shutdown complete!")
        Behaviors.stopped
    }

}
