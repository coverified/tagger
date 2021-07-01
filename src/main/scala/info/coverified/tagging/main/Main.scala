/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 * */

package info.coverified.tagging.main

import akka.actor.typed.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import info.coverified.graphql.GraphQLConnector.{DummySupervisorGraphQLConnector, ZIOSupervisorGraphQLConnector}
import info.coverified.tagging.Supervisor

import scala.util.{Failure, Success}

object Main extends LazyLogging {

  def main(args: Array[String]): Unit = {

    Config() match {
      case Failure(exception) =>
        logger.error("Parsing config failed.", exception)
        System.exit(1)
      case Success(cfg) =>
        val system: ActorSystem[Supervisor.TaggerSupervisorEvent] =
          ActorSystem(Supervisor(), "Tagger")
        system ! Supervisor.Init(
          cfg,
          ZIOSupervisorGraphQLConnector(cfg.graphQLApi, cfg.authSecret)
        )
        system ! Supervisor.StartTagging

    }

  }
}
