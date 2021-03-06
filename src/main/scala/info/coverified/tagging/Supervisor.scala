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
  AITagView,
  SupervisorGraphQLConnector,
  ZIOTaggerGraphQLConnector
}
import info.coverified.tagging.Tagger.{
  GracefulShutdown,
  HandleEntriesResponse,
  TaggerData,
  TaggerEvent
}
import info.coverified.tagging.ai.AiConnector.TaggerAiConnector
import info.coverified.tagging.main.Config
import akka.actor.typed.scaladsl.AskPattern._
import info.coverified.graphql.schema.CoVerifiedClientSchema.Language.LanguageView
import io.sentry.Sentry

import java.util.Date
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt
import scala.language.{existentials, postfixOps}
import scala.util.{Failure, Success}

object Supervisor extends LazyLogging {

  // events
  sealed trait TaggerSupervisorEvent

  final case class Init(cfg: Config, graphQL: SupervisorGraphQLConnector)
      extends TaggerSupervisorEvent // todo replace with interfaces for easier testing

  final case object StartTagging extends TaggerSupervisorEvent

  private type Tag = String
  private type Language = String

  private final case class EntryMeta(aiTags: Set[Tag], languages: Set[Language])
      extends TaggerSupervisorEvent

  private final case class TaggingFailed(exception: Throwable)
      extends TaggerSupervisorEvent

  private final case class Persisted(entries: Int) extends TaggerSupervisorEvent

  private final case class PersistenceFailed(throwable: Throwable)
      extends TaggerSupervisorEvent

  private final case object IdleTimeout extends TaggerSupervisorEvent

  private final case object DelayedTagging extends TaggerSupervisorEvent

  // data
  private final case class TaggerSupervisorData(
      cfg: Config, // todo cfg replace with interfaces for easier testing
      graphQL: SupervisorGraphQLConnector,
      workerPool: ActorRef[TaggerEvent],
      existingAiTags: Set[AITagView],
      existingLanguages: Set[LanguageView],
      taggingStartDate: Long = System.currentTimeMillis(),
      globalStartDate: Long = System.currentTimeMillis(),
      entryMetaStore: Vector[EntryMeta] = Vector.empty,
      processedEntries: Long = 0,
      retries: Int = 0
  ) {
    def clean: TaggerSupervisorData =
      this.copy(
        processedEntries = 0,
        retries = 0
      )

    def retry: TaggerSupervisorData =
      this.copy(
        processedEntries = 0,
        retries = this.retries + 1
      )
  }

  private final val SHUTDOWN_TIMEOUT = 30 seconds
  private final val MAX_RETRY_NO = 5

  def apply(): Behavior[TaggerSupervisorEvent] = uninitialized()

  private def uninitialized(): Behavior[TaggerSupervisorEvent] = {
    Behaviors.withStash(100) { msgBuffer =>
      Behaviors.receive {
        case (ctx, Init(cfg, graphQLConnector: SupervisorGraphQLConnector)) =>
          logger.info("Initializing Tagger ...")
          // todo scheduler

          // idle timeout to shutdown after some time
          if (cfg.internalScheduleInterval == -1) {
            val overallTimeout = cfg.batchDelay
              .map(_.millis + SHUTDOWN_TIMEOUT)
              .getOrElse(SHUTDOWN_TIMEOUT)
            ctx.setReceiveTimeout(overallTimeout, IdleTimeout)
          }

          // spawn router pool
          val workerPool = ctx.spawn(
            Routers
              .pool(cfg.noOfConcurrentWorker)(
                Tagger(
                  TaggerData(
                    cfg.batchSize,
                    TaggerAiConnector(cfg.kiApi.toString()),
                    ZIOTaggerGraphQLConnector(cfg.graphQLApi, cfg.authSecret)
                  )
                )
              )
              .withRoundRobinRouting()
              .withBroadcastPredicate(_.isInstanceOf[GracefulShutdown.type]),
            "tagger-pool"
          )
          ctx.watch(workerPool)

          // query existing aitags
          logger.info("Querying existing aiTags ...")
          val existingAiTags = graphQLConnector.queryAllExistingAiTags
          logger.info(s"Found ${existingAiTags.size} AiTags in database.")

          // query existing languages
          logger.info("Querying existing languages ...")
          val existingLanguages = graphQLConnector.queryAllExistingLanguages
          logger.info(s"Found ${existingLanguages.size} languages in database.")

          logger.info("Initialization complete!")
          msgBuffer.unstashAll(
            idle(
              TaggerSupervisorData(
                cfg,
                graphQLConnector,
                workerPool,
                existingAiTags,
                existingLanguages
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
            startTagging(
              data.workerPool,
              data.cfg.batchSize,
              data.cfg.noOfConcurrentWorker
            )(ctx)
            tagging(
              data.copy(taggingStartDate = System.currentTimeMillis()),
              ctx,
              msgBuffer
            )
          case IdleTimeout =>
            shutdown(data, ctx)
            Behaviors.stopped
          case invalid =>
            logger.warn(s"Received invalid msg '$invalid' when in idle.")
            Behaviors.unhandled
        }
    }

  private def tagging(
      data: TaggerSupervisorData,
      ctx: ActorContext[TaggerSupervisorEvent],
      msgBuffer: StashBuffer[TaggerSupervisorEvent]
  ): Behavior[TaggerSupervisorEvent] =
    Behaviors.receiveMessage {
      case StartTagging =>
        logger.info(
          "Received start tagging cmd but still running a tagging session. Stashing away!"
        )
        msgBuffer.stash(StartTagging)
        Behaviors.same
      case entryMeta: EntryMeta =>
        // all replies received; mutate aiTags + languages + let worker persist entities
        logger.info("All answers from worker received!")
        val newAiTags =
          entryMeta.aiTags.filterNot(data.existingAiTags.flatMap(_.name))
        val newLanguages =
          entryMeta.languages.filterNot(data.existingLanguages.flatMap(_.name))
        val updatedData =
          handleNewLanguages(handleNewAiTags(data, newAiTags), newLanguages)

        persistEntries(
          updatedData.existingAiTags,
          updatedData.existingLanguages,
          updatedData.workerPool,
          updatedData.cfg.noOfConcurrentWorker
        )(ctx)
        tagging(updatedData, ctx, msgBuffer)

      case TaggingFailed(exception) =>
        // TagEntries(...) cmd failed
        logger.error("AiTagging failed with exception: ", exception)
        Sentry.captureException(exception)
        if (data.retries < MAX_RETRY_NO) {
          logger.info(s"Retrying ... (current retry no: ${data.retries})")
          startTagging(
            data.workerPool,
            data.cfg.batchSize,
            data.cfg.noOfConcurrentWorker
          )(ctx)
          tagging(data.retry, ctx, msgBuffer)
        } else {
          logger.warn("Max no of retries reached. Going back to idle!")
          msgBuffer.unstashAll(idle(data.clean, msgBuffer))
        }

      case persisted: Persisted =>
        processPersisted(
          persisted,
          data,
          data.cfg.batchSize,
          data.cfg.noOfConcurrentWorker
        ) match {
          case (updatedData, allEntriesTagged) if !allEntriesTagged =>
            // not done yet, start next tagging round
            data.cfg.batchDelay match {
              case Some(batchDelay) =>
                logger.info(
                  s"Processed ${updatedData.processedEntries} entries so far, but there are still some left. " +
                    s"Next batch tagging round is delayed by $batchDelay millis. Stay calm."
                )
                ctx.scheduleOnce(batchDelay millis, ctx.self, DelayedTagging)
              case None =>
                logger.info(
                  s"Processed ${updatedData.processedEntries} entries so far, but there are still some left. " +
                    s"Starting next tagging round ..."
                )
                startTagging(
                  data.workerPool,
                  data.cfg.batchSize,
                  data.cfg.noOfConcurrentWorker
                )(ctx)
            }

            tagging(updatedData, ctx, msgBuffer)

          case (updatedData, _) =>
            // tagging for all entities done, cleanup, unstash and return to idle
            logger.info(
              s"Tagging process completed! Tagged ${updatedData.processedEntries} entries!"
            )
            logger.info("--------- Tagging Stats ---------")
            logger.info(s"Start: ${new Date(data.taggingStartDate)}")
            logger.info(
              s"End: ${new Date(System.currentTimeMillis())}"
            )
            logger.info(
              s"Duration: ${(System.currentTimeMillis() - data.taggingStartDate) / 1000}s"
            )
            logger.info("---------------------------------")

            msgBuffer.unstashAll(idle(updatedData.clean, msgBuffer))
        }

      case DelayedTagging =>
        logger.info("Starting next tagging round ...")
        startTagging(
          data.workerPool,
          data.cfg.batchSize,
          data.cfg.noOfConcurrentWorker
        )(ctx)
        tagging(data, ctx, msgBuffer)

      case PersistenceFailed(exception) =>
        // Persisted(...) cmd failed
        // a potential retry must be done here
        logger.error("Persisting failed with exception: ", exception)
        Sentry.captureException(exception)
        if (data.retries < MAX_RETRY_NO) {
          logger.info(s"Retrying ... (current retry no: ${data.retries})")

          persistEntries(
            data.existingAiTags,
            data.existingLanguages,
            data.workerPool,
            data.cfg.noOfConcurrentWorker
          )(ctx)
          tagging(data.retry, ctx, msgBuffer)
        } else {
          logger.warn("Max no of retries reached. Going back to idle!")
          msgBuffer.unstashAll(idle(data.clean, msgBuffer))
        }

      case invalidMsg =>
        logger.error(
          s"Invalid msg received during tagging process: '$invalidMsg'"
        )
        tagging(data, ctx, msgBuffer)
    }

  private def startTagging(
      workerPool: ActorRef[TaggerEvent],
      batchSize: Int,
      noOfWorkers: Int
  )(
      implicit
      ctx: ActorContext[TaggerSupervisorEvent],
      timeout: Timeout = 60 seconds
  ): Unit = {
    implicit val system: ActorSystem[Nothing] = ctx.system
    implicit val executionContextExecutor: ExecutionContextExecutor =
      ctx.executionContext

    ctx.pipeToSelf(
      Future
        .sequence(
          (0 until noOfWorkers)
            .foldLeft((0, Vector.empty[Future[HandleEntriesResponse]])) {
              case ((skip, requests), _) =>
                val ask =
                  workerPool.ask(replyTo => Tagger.HandleEntries(skip, replyTo))
                (skip + batchSize, requests :+ ask)
            }
            ._2
        )
    ) {
      case Failure(exception) =>
        TaggingFailed(exception)
      case Success(handlingResponses) =>
        EntryMeta(
          handlingResponses.flatMap(_.aiTags).toSet,
          handlingResponses.flatMap(_.languages).toSet
        )
    }

  }

  private def persistEntries(
      allTags: Set[AITagView],
      allLanguages: Set[LanguageView],
      workerPool: ActorRef[TaggerEvent],
      noOfWorkers: Int
  )(
      implicit
      ctx: ActorContext[TaggerSupervisorEvent],
      timeout: Timeout = 60 seconds
  ): Unit = {
    implicit val system: ActorSystem[Nothing] = ctx.system
    implicit val executionContextExecutor: ExecutionContextExecutor =
      ctx.executionContext

    ctx.pipeToSelf(
      Future
        .sequence(
          (0 until noOfWorkers)
            .map(_ => {
              workerPool.ask(
                replyTo => Tagger.PersistEntries(allTags, allLanguages, replyTo)
              )
            })
            .map(_.transform(Success(_)))
        )
        .map { tries =>
          val (failures, successes) = tries.partitionMap(_.toEither)
          failures.headOption match {
            case Some(exception: Throwable) =>
              throw exception
            case None =>
              successes.map(_.entries).sum
          }
        }
    ) {
      case Failure(exception) =>
        PersistenceFailed(exception)
      case Success(noOfPersistedEntries) =>
        Persisted(noOfPersistedEntries)
    }
  }

  private def processPersisted(
      persisted: Persisted,
      data: TaggerSupervisorData,
      batchSize: Int,
      noOfWorkers: Int
  ): (TaggerSupervisorData, Boolean) = {

    // all entries are tagged if sum of entries < worker * batchSize
    val allEntriesPersisted = persisted.entries < batchSize * noOfWorkers
    (
      data.copy(processedEntries = data.processedEntries + persisted.entries),
      allEntriesPersisted
    )
  }

  private def handleNewAiTags(
      data: TaggerSupervisorData,
      newTags: Set[Tag]
  ): TaggerSupervisorData =
    if (newTags.nonEmpty) {
      logger.info(s"Mutating ${newTags.size} new tags ...")
      val newTagViews: Set[AITagView] = data.graphQL.mutateAiTags(newTags)
      val allTags = newTagViews ++ data.existingAiTags
      logger.info(s"Mutation done. Overall tag no: ${allTags.size}")
      data.copy(existingAiTags = allTags)
    } else data

  private def handleNewLanguages(
      data: TaggerSupervisorData,
      newLanguages: Set[Language]
  ): TaggerSupervisorData =
    if (newLanguages.nonEmpty) {
      logger.info(s"Mutating ${newLanguages.size} new languages ...")
      val newLanguageViews: Set[LanguageView] =
        data.graphQL.mutateLanguages(newLanguages)
      val allLanguages = newLanguageViews ++ data.existingLanguages
      logger.info(s"Mutation done. Overall language no: ${allLanguages.size}")
      data.copy(existingLanguages = allLanguages)
    } else data

  private def shutdown(
      data: TaggerSupervisorData,
      ctx: ActorContext[TaggerSupervisorEvent]
  ): Unit = {
    logger.info("Tagger shutdown initiated ...")
    logger.info("--------- Overall Stats ---------")
    logger.info(s"Start: ${new Date(data.globalStartDate)}")
    logger.info(
      s"End: ${new Date(System.currentTimeMillis() - SHUTDOWN_TIMEOUT.toMillis)}"
    )
    logger.info(
      s"Duration: ${(System.currentTimeMillis() - SHUTDOWN_TIMEOUT.toMillis - data.globalStartDate) / 1000}s"
    )
    logger.info("---------------------------------")

    ctx.cancelReceiveTimeout()

    data.graphQL.close()

    data.workerPool ! GracefulShutdown
    ctx.unwatch(data.workerPool)

    ctx.system.terminate()
  }

}
