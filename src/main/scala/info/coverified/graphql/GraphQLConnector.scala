/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.graphql

import com.typesafe.scalalogging.LazyLogging
import info.coverified.graphql.schema.CoVerifiedClientSchema.Entry.EntryView
import info.coverified.graphql.schema.CoVerifiedClientSchema.Language.LanguageView
import info.coverified.graphql.schema.CoVerifiedClientSchema.{
  AITag,
  AITagCreateInput,
  AITagWhereInput,
  AITagsCreateInput,
  ArticleTag,
  ArticleTagWhereInput,
  EntriesUpdateInput,
  Entry,
  EntryWhereInput,
  Language,
  LanguageCreateInput,
  LanguageWhereInput,
  LanguagesCreateInput,
  Mutation,
  Query,
  Source,
  Tag,
  TagWhereInput,
  Url,
  _QueryMeta
}
import io.sentry.Sentry
import org.asynchttpclient.DefaultAsyncHttpClient
import sttp.capabilities.zio.ZioStreams
import sttp.client3.SttpBackend
import sttp.client3.asynchttpclient.zio.AsyncHttpClientZioBackend
import sttp.model.Uri
import zio.{Task, ZIO}

trait GraphQLConnector {

  protected val authSecret: String

}

object GraphQLConnector {

  type AITagView = AITag.AITagView

  type EntryView =
    Entry.EntryView[Url.UrlView[Source.SourceView], Tag.TagView, _QueryMeta._QueryMetaView, ArticleTag.ArticleTagView, _QueryMeta._QueryMetaView, AITag.AITagView, _QueryMeta._QueryMetaView, Language.LanguageView]

  sealed trait SupervisorGraphQLConnector extends GraphQLConnector {

    def queryAllExistingAiTags: Set[AITagView]

    def queryAllExistingLanguages: Set[LanguageView]

    def mutateAiTags(tags: Set[String]): Set[AITagView]

    def mutateLanguages(languages: Set[String]): Set[Language.LanguageView]

    def close(): Unit
  }

  final case class ZIOSupervisorGraphQLConnector(
      private val apiUrl: Uri,
      protected val authSecret: String
  ) extends SupervisorGraphQLConnector
      with LazyLogging {

    private val runtime: zio.Runtime[zio.ZEnv] = zio.Runtime.default
    private val client = new DefaultAsyncHttpClient()
    private val backend: SttpBackend[Task, ZioStreams] =
      AsyncHttpClientZioBackend.usingClient(
        runtime,
        client
      )

    private val existingTagsQuery =
      Query.allAITags(AITagWhereInput(), skip = 0)(AITag.view)
    private val createTagsMutation =
      (tags: Set[String]) => // todo language of tags
        Mutation.createAITags(
          Some({
            tags
              .map(
                name =>
                  Some(
                    AITagsCreateInput(
                      Some(
                        AITagCreateInput(
                          name = Some(name)
                        )
                      )
                    )
                  )
              )
              .toList
          })
        )(AITag.view)

    private val existingLanguagesQuery =
      Query.allLanguages(LanguageWhereInput(), skip = 0)(Language.view)
    private val createLanguagesMutation = (languages: Set[String]) =>
      Mutation.createLanguages(
        Some({
          languages
            .map(
              name =>
                Some(
                  LanguagesCreateInput(
                    Some(
                      LanguageCreateInput(
                        name = Some(name)
                      )
                    )
                  )
                )
            )
            .toList
        })
      )(Language.view)

    override def queryAllExistingLanguages: Set[LanguageView] =
      runtime.unsafeRun(
        existingLanguagesQuery
          .toRequest(apiUrl)
          .header("x-coverified-internal-auth", authSecret)
          .header("x-coverified-internal-auth", authSecret)
          .send(backend)
          .foldM(
            ex => {
              logger.error("Cannot query existing languages! Exception:", ex)
              Sentry.captureException(ex)
              ZIO.succeed(Set.empty[Language.LanguageView])
            },
            suc =>
              ZIO.succeed(
                suc.body match {
                  case Left(error) =>
                    Sentry.captureException(error)
                    logger.error(
                      "Error returned from API during aiTag mutation execution! Exception:",
                      error
                    )
                    Set.empty
                  case Right(allLanguages) =>
                    allLanguages.map(_.toSet).getOrElse(Set.empty)
                }
              )
          )
      )

    override def queryAllExistingAiTags: Set[AITag.AITagView] =
      runtime
        .unsafeRun(
          existingTagsQuery
            .toRequest(apiUrl)
            .header("x-coverified-internal-auth", authSecret)
            .header("x-coverified-internal-auth", authSecret)
            .send(backend)
            .map(_.body)
            .absolve
        )
        .getOrElse(Set.empty)
        .toSet

    override def mutateAiTags(
        tags: Set[String]
    ): Set[AITag.AITagView] =
      runtime.unsafeRun(
        createTagsMutation(tags)
          .toRequest(apiUrl)
          .header("x-coverified-internal-auth", authSecret)
          .send(backend)
          .foldM(
            ex => {
              logger.error(
                "Cannot send aiTag mutation request to API! Exception:",
                ex
              )
              Sentry.captureException(ex)
              ZIO.succeed(Set.empty[AITag.AITagView])
            },
            suc =>
              ZIO.succeed(
                suc.body match {
                  case Left(err) =>
                    Sentry.captureException(err)
                    logger.error(
                      "Error returned from API during aiTag mutation execution! Exception:",
                      err
                    )
                    Set.empty
                  case Right(mutatedTags) =>
                    mutatedTags.map(_.flatten.toSet).getOrElse(Set.empty)
                }
              )
          )
      )

    override def close(): Unit = {
      client.close()
      backend.close()
    }

    override def mutateLanguages(
        languages: Set[String]
    ): Set[Language.LanguageView] = runtime.unsafeRun(
      createLanguagesMutation(languages)
        .toRequest(apiUrl)
        .header("x-coverified-internal-auth", authSecret)
        .send(backend)
        .foldM(
          ex => {
            logger.error(
              "Cannot send language mutation request to API! Exception:",
              ex
            )
            Sentry.captureException(ex)
            ZIO.succeed(Set.empty[Language.LanguageView])
          },
          suc =>
            ZIO.succeed(
              suc.body match {
                case Left(err) =>
                  Sentry.captureException(err)
                  logger.error(
                    "Error returned from API during language mutation execution! Exception:",
                    err
                  )
                  Set.empty
                case Right(mutateLanguages) =>
                  mutateLanguages.map(_.flatten.toSet).getOrElse(Set.empty)
              }
            )
        )
    )
  }

  final case class DummySupervisorGraphQLConnector()
      extends SupervisorGraphQLConnector
      with LazyLogging {

    override def queryAllExistingAiTags: Set[AITag.AITagView] = {

      logger.info("Querying existing tags in dummy connector!")

      Set.empty
    }

    override def mutateAiTags(
        tags: Set[String]
    ): Set[AITag.AITagView] = {

      logger.info("mutate tags in dummy connector!")

      Set.empty
    }

    override val authSecret: String = "NO_AUTH_REQUIRED"

    override def close(): Unit = {}

    override def queryAllExistingLanguages: Set[LanguageView] = {
      logger.info("Querying existing languages in dummy connector!")

      Set.empty
    }

    override def mutateLanguages(
        languages: Set[String]
    ): Set[Language.LanguageView] = {
      logger.info("mutate languages in dummy connector!")

      Set.empty
    }
  }

  sealed trait TaggerGraphQLConnector extends GraphQLConnector {

    def queryEntries(
        skip: Int,
        first: Int,
        filter: EntryWhereInput
    ): Set[EntryView]

    def updateEntriesWithAiTags(
        entryUuids: Seq[EntriesUpdateInput]
    ): Option[Vector[EntryView]]

    def close(): Unit
  }

  final case class ZIOTaggerGraphQLConnector(
      private val apiUrl: Uri,
      protected val authSecret: String
  ) extends TaggerGraphQLConnector
      with LazyLogging {

    private val runtime: zio.Runtime[zio.ZEnv] = zio.Runtime.default
    private val client = new DefaultAsyncHttpClient()
    private val backend: SttpBackend[Task, ZioStreams] =
      AsyncHttpClientZioBackend.usingClient(
        runtime,
        client
      )

    private val fullEntryViewInnerSelection = Entry.view(
      aiTagsWhere = AITagWhereInput(),
      aiTagsSkip = 0,
      _aiTagsMetaWhere = AITagWhereInput(),
      _aiTagsMetaSkip = 0,
      aiTagsCountWhere = AITagWhereInput(),
      tagsWhere = TagWhereInput(),
      tagsSkip = 0,
      _tagsMetaWhere = TagWhereInput(),
      _tagsMetaSkip = 0,
      tagsCountWhere = TagWhereInput(),
      articleTagsWhere = ArticleTagWhereInput(),
      articleTagsSkip = 0,
      _articleTagsMetaWhere = ArticleTagWhereInput(),
      _articleTagsMetaSkip = 0,
      articleTagsCountWhere = ArticleTagWhereInput()
    )(
      Url.view(Source.view),
      Tag.view,
      _QueryMeta.view,
      ArticleTag.view,
      _QueryMeta.view,
      AITag.view,
      _QueryMeta.view,
      Language.view
    )

    private def entriesQuery =
      (skip: Int, first: Int, filter: EntryWhereInput) =>
        Query.allEntries(
          where = filter,
          first = Some(first),
          skip = skip
        )(fullEntryViewInnerSelection)

    private val updateEntriesWithTagsMutation =
      (entries: Seq[EntriesUpdateInput]) =>
        Mutation.updateEntries(
          Some(
            entries.map(Some(_)).toList
          )
        )(fullEntryViewInnerSelection)

    override def queryEntries(
        skip: Int,
        first: Int,
        filter: EntryWhereInput
    ): Set[EntryView] = {
      runtime.unsafeRun(
        entriesQuery(skip, first, filter)
          .toRequest(apiUrl)
          .header("x-coverified-internal-auth", authSecret)
          .send(backend)
          .foldM(
            ex => {
              logger.error("Cannot query entries! Exception:", ex)
              Sentry.captureException(ex)
              ZIO.succeed(Set.empty[EntryView])
            },
            suc =>
              ZIO
                .succeed(
                  suc.body match {
                    case Left(error) =>
                      logger.error(
                        "API returned error on entity query: ",
                        error
                      )
                      Sentry.captureException(error)
                      Set.empty[EntryView]
                    case Right(entries) =>
                      entries.map(_.toSet).getOrElse(Set.empty)
                  }
                )
          )
      )
    }

    override def close(): Unit = {
      logger.info("Trying to close graphQL connection client ...")
      client.close()
      logger.info("Trying to close graphQL connection backend ...")
      backend.close()
    }

    override def updateEntriesWithAiTags(
        entryUuids: Seq[EntriesUpdateInput]
    ): Option[Vector[EntryView]] = {
      runtime.unsafeRun(
        updateEntriesWithTagsMutation(entryUuids)
          .toRequest(apiUrl)
          .header("x-coverified-internal-auth", authSecret)
          .send(backend)
          .foldM(
            ex => {
              logger.error(
                "Cannot send entry aiTag update request to API! Exception:",
                ex
              )
              Sentry.captureException(ex)
              ZIO.succeed(None)
            },
            suc =>
              ZIO.succeed(
                suc.body match {
                  case Left(err) =>
                    Sentry.captureException(err)
                    logger.error(
                      "Error returned from API during entry update with tags mutation execution! Exception:",
                      err
                    )
                    None
                  case Right(mutatedEntry) =>
                    mutatedEntry.map(_.flatten.toVector)
                }
              )
          )
      )
    }
  }

  final case class DummyTaggerGraphQLConnector()
      extends TaggerGraphQLConnector
      with LazyLogging {

    // todo JH remove
    def queryEntries(
        skip: Int,
        first: Int,
        filter: EntryWhereInput
    ): Set[EntryView] =
      throw new RuntimeException(
        s"Query Entries is not implemented in ${getClass.getSimpleName}!"
      )

    def updateEntryWithTags(
        entryUuid: String,
        tagUuids: Set[String]
    ): Option[EntryView] = {

      throw new RuntimeException(
        s"Update entry is not implemented in ${getClass.getSimpleName}!"
      )
    }

    override val authSecret: String = "NO_AUTH_REQUIRED"

    override def close(): Unit = {}

    override def updateEntriesWithAiTags(
        entryUuids: Seq[EntriesUpdateInput]
    ): Option[Vector[EntryView]] =
      throw new RuntimeException(
        s"Update entries is not implemented in ${getClass.getSimpleName}!"
      )
  }

}
