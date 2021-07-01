/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
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

import scala.concurrent.{ExecutionContextExecutor, Future}
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
  private final case class TaggingFailed(exception: Throwable)
      extends TaggerSupervisorEvent

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
            ctx.pipeToSelf(
              startTagging(data, data.cfg.noOfConcurrentWorker, ctx)
            ) {
              case Failure(exception) =>
                TaggingFailed(exception)
              case Success(tags) =>
                Tags(tags.toSet)
            }
            tagging(data, ctx, msgBuffer)
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
        // all replies received; mutate tags + let worker persist entities
        val newTags = tagData.tags.filterNot(data.existingTags.flatMap(_.name))
        val updatedData = if (newTags.nonEmpty) {
          logger.info(s"Mutating ${newTags.size} new tags ...")
          val newTagViews: Set[TagView] = data.graphQL.mutateTags(newTags)
          val allTags = newTagViews ++ data.existingTags
          logger.info(s"Mutation done. Overall tag no: ${allTags.size}")
          data.copy(existingTags = allTags)
        } else data

        persistEntries(updatedData.existingTags, updatedData, ctx)
        tagging(updatedData, ctx, msgBuffer)

      case TaggingFailed(exception) =>
        // TagEntries(...) cmd failed
        // a potential retry must be done here
        logger.error("Tagging failed with exception: ", exception) // todo sentry integration
        Behaviors.same

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
            ctx.pipeToSelf(
              startTagging(
                updatedData.copy(persistenceStore = Vector.empty),
                data.cfg.noOfConcurrentWorker,
                ctx
              )
            ) {
              case Failure(exception) =>
                TaggingFailed(exception)
              case Success(tags) =>
                Tags(tags.toSet)
            }
            tagging(updatedData, ctx, msgBuffer)
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
  )(implicit timeout: Timeout = 5 seconds): Future[Vector[Tag]] = {

    // todo move up
    import akka.actor.typed.scaladsl.AskPattern._
    implicit val ec: ExecutionContextExecutor = ctx.system.executionContext
    implicit val system: ActorSystem[Nothing] = ctx.system

    Future
      .sequence(
        (0 until noOfWorkers)
          .foldLeft((0, Vector.empty[Future[TagEntriesResponse]])) {
            case ((skip, futures), _) =>
              val fut =
                data.workerPool.ask(replyTo => Tagger.TagEntries(skip, replyTo))
              (skip + data.cfg.batchSize, futures :+ fut)
          }
          ._2
      )
      .map(_.flatMap(_.tags))
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
