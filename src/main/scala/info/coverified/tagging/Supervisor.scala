/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, Routers, StashBuffer}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import info.coverified.graphql.GraphQLConnector.{
  SupervisorGraphQLConnector,
  TagView,
  ZIOTaggerGraphQLConnector
}
import info.coverified.tagging.Tagger.{
  PersistEntriesResponse,
  TagEntriesResponse,
  TaggerData,
  TaggerEvent
}
import info.coverified.tagging.ai.AiConnector.DummyTaggerAiConnector
import info.coverified.tagging.main.Config

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}

object Supervisor extends LazyLogging {

  // events
  sealed trait TaggerSupervisorEvent

  final case class Init(cfg: Config, graphQL: SupervisorGraphQLConnector)
      extends TaggerSupervisorEvent // todo replace with interfaces for easier testing

  final case object StartTagging extends TaggerSupervisorEvent

  private type Tag = String

  private final case class Tags(tags: Set[Tag]) extends TaggerSupervisorEvent

  private final case class Persisted(entries: Int) extends TaggerSupervisorEvent

  // data
  private final case class TaggerSupervisorData(
      cfg: Config, // todo cfg replace with interfaces for easier testing
      graphQL: SupervisorGraphQLConnector,
      workerPool: ActorRef[TaggerEvent],
      existingTags: Set[TagView],
      tagStore: Vector[Tags] = Vector.empty,
      persistenceStore: Vector[Persisted] = Vector.empty,
      processedEntries: Long = 0
  ) {
    def clean: TaggerSupervisorData =
      this.copy(
        tagStore = Vector.empty,
        persistenceStore = Vector.empty,
        processedEntries = 0
      )
  }

  def apply(): Behavior[TaggerSupervisorEvent] = uninitialized()

  private def uninitialized(): Behavior[TaggerSupervisorEvent] = {
    Behaviors.withStash(100) { msgBuffer =>
      Behaviors.receive {
        case (ctx, Init(cfg, graphQLConnector: SupervisorGraphQLConnector)) =>
          logger.info("Initializing Tagger ...")
          // todo scheduler

          // spawn router pool
          val workerPool = ctx.spawn(
            Routers
              .pool(cfg.noOfConcurrentWorker)(
                Tagger(
                  TaggerData(
                    cfg.batchSize,
                    DummyTaggerAiConnector(),
                    ZIOTaggerGraphQLConnector(cfg.graphQLApi, cfg.authSecret)
                  )
                )
              )
              .withRoundRobinRouting(),
            "tagger-pool"
          )

          // query existing tags
          logger.info("Querying existing tags ...")
          val existingTags = graphQLConnector.queryAllExistingTags
          logger.info(s"Found ${existingTags.size} tags in database.")

          logger.info("Initialization complete!")
          msgBuffer.unstashAll(
            idle(
              TaggerSupervisorData(
                cfg,
                graphQLConnector,
                workerPool,
                existingTags
              ),
              msgBuffer
            )
          )
        case (_, invalidMsg) =>
          logger.error(
            s"Cannot process msg '$invalidMsg'. Please initialize tagger first!"
          )
          msgBuffer.stash(invalidMsg)
          Behaviors.same
      }
    }
  }

  private def idle(
      data: TaggerSupervisorData,
      msgBuffer: StashBuffer[TaggerSupervisorEvent]
  ): Behavior[TaggerSupervisorEvent] =
    Behaviors.receive {
      case (ctx, msg) =>
        msg match {
          case Init(_, _) =>
            logger.error("Tagger is already initialized! Ignoring init msg!")
            Behaviors.same
          case StartTagging =>
            logger.info("Starting tagging process ...")
            val updatedData =
              startTagging(data, data.cfg.noOfConcurrentWorker, ctx)
            tagging(updatedData, ctx, msgBuffer)
          case invalid =>
            logger.warn(s"Received invalid msg '$invalid' when in idle.")
            Behaviors.unhandled
        }
    }

  private def tagging(
      data: TaggerSupervisorData,
      ctx: ActorContext[TaggerSupervisorEvent],
      msgBuffer: StashBuffer[TaggerSupervisorEvent]
  )(implicit timeout: Timeout = 5 seconds): Behavior[TaggerSupervisorEvent] =
    Behaviors.receiveMessage {
      case StartTagging =>
        logger.info(
          "Received start tagging cmd but still running a tagging session. Stashing away!"
        )
        msgBuffer.stash(StartTagging)
        Behaviors.same
      case tagData: Tags =>
        processNewTags(data, tagData) match {
          case (updatedData, None) =>
            // still waiting for replies from tagger instances, just stay here
            tagging(updatedData, ctx, msgBuffer)
          case (updatedData, Some(tags)) =>
            // all replies received; mutate tags + let worker persist entities
            val allTags = if (tags.nonEmpty) {
              logger.info(s"Mutating ${tags.size} new tags ...")
              val newTags: Set[TagView] = data.graphQL.mutateTags(tags)
              val allTags = newTags ++ updatedData.existingTags
              logger.info(s"Mutation done. Overall tag no: ${allTags.size}")
              allTags
            } else updatedData.existingTags

            persistEntries(allTags, updatedData, ctx)
            tagging(updatedData.copy(existingTags = allTags), ctx, msgBuffer)
        }

      case persisted: Persisted =>
        processPersisted(data, persisted) match {
          case (updatedData, None) =>
            // still waiting for persist cmd results from workers, just stay here
            tagging(updatedData, ctx, msgBuffer)
          case (updatedData, Some(allEntitiesTagged)) if !allEntitiesTagged =>
            // not done yet, start next tagging round
            logger.info(
              s"Processed ${updatedData.processedEntries} entries so far, but there are still some left. " +
                s"Starting next tagging round ..."
            )
            tagging(
              startTagging(
                updatedData.copy(persistenceStore = Vector.empty),
                data.cfg.noOfConcurrentWorker,
                ctx
              ),
              ctx,
              msgBuffer
            )
          case (updatedData, Some(_)) =>
            // tagging for all entities done, cleanup, unstash and return to idle
            logger.info(
              s"Tagging process completed! Tagged ${updatedData.processedEntries} entries!"
            )
            updatedData.clean
            msgBuffer.unstashAll(idle(updatedData, msgBuffer))
        }

      case invalidMsg =>
        logger.error(
          s"Invalid msg received during tagging process: '$invalidMsg'"
        )
        tagging(data, ctx, msgBuffer)
    }

  private def startTagging(
      data: TaggerSupervisorData,
      noOfWorkers: Int,
      ctx: ActorContext[TaggerSupervisorEvent]
  )(implicit timeout: Timeout = 5 seconds): TaggerSupervisorData = {

    (0 until noOfWorkers).foldLeft(0)(
      (skip, _) => {
        ctx.ask(data.workerPool, Tagger.TagEntries(skip, _)) {
          case Success(TagEntriesResponse(tags)) =>
            Tags(tags)
          case Failure(_) => ??? // todo JH
        }
        skip + data.cfg.batchSize
      }
    )
    data
  }

  private def processNewTags(
      data: TaggerSupervisorData,
      tagsWithEntryNo: Tags
  ): (TaggerSupervisorData, Option[Set[Tag]]) = {
    val allReceivedTags = data.tagStore :+ tagsWithEntryNo

    Option.when(allReceivedTags.size == data.cfg.noOfConcurrentWorker) {
      // build mutations for new tags
      allReceivedTags
        .flatMap(_.tags)
        .filterNot(data.existingTags.flatMap(_.name))
    } match {
      case Some(tags) =>
        // clean tag store + return mutations
        (data.copy(tagStore = Vector.empty), Some(tags.toSet))
      case None =>
        // not yet all results
        (data.copy(tagStore = allReceivedTags), None)
    }
  }

  private def persistEntries(
      allTags: Set[TagView],
      data: TaggerSupervisorData,
      ctx: ActorContext[TaggerSupervisorEvent]
  )(implicit timeout: Timeout = 5 seconds): Unit = {
    (0 until data.cfg.noOfConcurrentWorker).foreach(
      _ =>
        ctx.ask(data.workerPool, Tagger.PersistEntries(allTags, _)) {
          case Success(PersistEntriesResponse(entries)) =>
            Persisted(entries)
          case Failure(_) => ??? // todo JH
        }
    )
  }

  private def processPersisted(
      data: TaggerSupervisorData,
      persisted: Persisted
  ): (TaggerSupervisorData, Option[Boolean]) = {
    val allPersistedEntries = data.persistenceStore :+ persisted
    val entryNo = allPersistedEntries.map(_.entries).sum
    Option.when(allPersistedEntries.size == data.cfg.noOfConcurrentWorker) {
      // all entries persisted, check if we have handled all entries
      // all entries are tagged if sum of entries < worker * batchSize
      entryNo < data.cfg.batchSize * data.cfg.noOfConcurrentWorker
    } match {
      case Some(allEntriesTagged) =>
        // all entries tagged
        // clean persistence store + return
        (
          data.copy(
            persistenceStore = Vector.empty,
            processedEntries = data.processedEntries + entryNo
          ),
          Some(allEntriesTagged)
        )
      case None =>
        // not yet all entries persisted
        (data.copy(persistenceStore = allPersistedEntries), None)
    }
  }
}
