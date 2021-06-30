/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.typesafe.scalalogging.LazyLogging
import info.coverified.graphql.GraphQLConnector.{
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
        val taggingResult: Vector[(Set[String], EntryView)] =
          entities.map(entity => (data.ai.queryTags(entity), entity))

        val updatedData = data.copy(currentTaggingResults = taggingResult)

        replyTo ! TagEntriesResponse(taggingResult.flatMap(_._1).toSet)

        apply(updatedData)

      case PersistEntries(tags, replyTo) =>
        // mutate entries
        val tagMap = tags
          .flatMap(tagView => tagView.name.map(name => name -> tagView.id))
          .toMap

        data.currentTaggingResults.foreach {
          case (tags, entry) =>
            val tagUuids = tags.flatMap(tagMap.get)
            data.graphQL.updateEntryWithTags(entry.id, tagUuids)
        }

        replyTo ! PersistEntriesResponse(data.currentTaggingResults.size)

        apply(data.copy(currentTaggingResults = Vector.empty))
    }

}
