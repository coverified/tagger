/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.graphql

import com.typesafe.scalalogging.LazyLogging
import info.coverified.graphql.schema.CoVerifiedClientSchema.{
  Entry,
  EntryUpdateInput,
  EntryWhereInput,
  Language,
  Mutation,
  Query,
  Source,
  Tag,
  TagCreateInput,
  TagRelateToManyInput,
  TagWhereInput,
  TagWhereUniqueInput,
  TagsCreateInput,
  Url,
  _QueryMeta
}
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

  type TagView = Tag.TagView[Language.LanguageView]

  type EntryView = Entry.EntryView[Url.UrlView[Source.SourceView], Tag.TagView[
    Language.LanguageView
  ], _QueryMeta._QueryMetaView, Language.LanguageView]

  sealed trait SupervisorGraphQLConnector extends GraphQLConnector {

    def queryAllExistingTags: Set[TagView]

    def mutateTags(tags: Set[String]): Set[TagView]

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
      Query.allTags(TagWhereInput(), skip = 0)(Tag.view(Language.view))
    private val createTagsMutation =
      (tags: Set[String]) => // todo language of tags
        Mutation.createTags(
          Some({
            tags
              .map(
                name =>
                  Some(
                    TagsCreateInput(
                      Some(
                        TagCreateInput(
                          name = Some(name)
                        )
                      )
                    )
                  )
              )
              .toList
          })
        )(Tag.view(Language.view))

    override def queryAllExistingTags: Set[Tag.TagView[Language.LanguageView]] =
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

    override def mutateTags(
        tags: Set[String]
    ): Set[Tag.TagView[Language.LanguageView]] =
      runtime.unsafeRun(
        createTagsMutation(tags)
          .toRequest(apiUrl)
          .header("x-coverified-internal-auth", authSecret)
          .send(backend)
          .foldM(
            ex => {
              logger.error(
                "Cannot send tag mutation request to API! Exception:",
                ex
              ) // todo sentry integration
              ZIO.succeed(Set.empty[Tag.TagView[Language.LanguageView]])
            },
            suc =>
              ZIO.succeed(
                suc.body match {
                  case Left(err) => // todo sentry integration
                    logger.error(
                      "Error returned from API during tag mutation execution! Exception:",
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
  }

  final case class DummySupervisorGraphQLConnector()
      extends SupervisorGraphQLConnector
      with LazyLogging {

    override def queryAllExistingTags
        : Set[Tag.TagView[Language.LanguageView]] = {

      logger.info("Querying existing tags in dummy connector!")

      Set.empty
    }

    override def mutateTags(
        tags: Set[String]
    ): Set[Tag.TagView[Language.LanguageView]] = {

      logger.info("mutate existing tags in dummy connector!")

      Set.empty
    }

    override val authSecret: String = "NO_AUTH_REQUIRED"

    override def close(): Unit = {}
  }

  sealed trait TaggerGraphQLConnector extends GraphQLConnector {

    def queryEntries(skip: Int, first: Int): Set[EntryView]

    def updateEntryWithTags(
        entryUuid: String,
        tagUuids: Set[String]
    ): Option[EntryView]

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
      tagsWhere = TagWhereInput(),
      tagsSkip = 0,
      _tagsMetaWhere = TagWhereInput(),
      _tagsMetaSkip = 0,
      tagsCountWhere = TagWhereInput()
    )(
      Url.view(Source.view),
      Tag.view(Language.view),
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

    private val updateEntryWithTagsMutation =
      (entryId: String, tagIds: Set[String]) =>
        Mutation.updateEntry(
          entryId,
          Some(
            EntryUpdateInput(
              hasBeenTagged = Some(true),
              tags = Some(
                TagRelateToManyInput(
                  connect = Some(
                    tagIds
                      .map(
                        tagid =>
                          Some(
                            TagWhereUniqueInput(
                              Some(tagid)
                            )
                          )
                      )
                      .toList
                  )
                )
              )
            )
          )
        )(fullEntryViewInnerSelection)

    override def queryEntries(skip: Int, first: Int): Set[EntryView] = {
      // filter for untagged entries
      val filter = EntryWhereInput(
        hasBeenTagged = Some(false)
      )
      runtime.unsafeRun(
        entriesQuery(skip, first, filter)
          .toRequest(apiUrl)
          .header("x-coverified-internal-auth", authSecret)
          .send(backend)
          .foldM(
            ex => {
              logger.error("Cannot query entries! Exception:", ex) // todo sentry integration
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
                      ) // todo sentry integration
                      Set.empty[EntryView]
                    case Right(entries) =>
                      entries.map(_.toSet).getOrElse(Set.empty)
                  }
                )
          )
      )
    }

    override def updateEntryWithTags(
        entryUuid: String,
        tagUuids: Set[String]
    ): Option[EntryView] = {

      runtime.unsafeRun(
        updateEntryWithTagsMutation(entryUuid, tagUuids)
          .toRequest(apiUrl)
          .header("x-coverified-internal-auth", authSecret)
          .send(backend)
          .foldM(
            ex => {
              logger.error(
                "Cannot send entry tag update request to API! Exception:",
                ex
              ) // todo sentry integration
              ZIO.succeed(None)
            },
            suc =>
              ZIO.succeed(
                suc.body match {
                  case Left(err) => // todo sentry integration
                    logger.error(
                      "Error returned from API during entry update with tags mutation execution! Exception:",
                      err
                    )
                    None
                  case Right(mutatedEntry) =>
                    mutatedEntry
                }
              )
          )
      )
    }

    override def close(): Unit = {
      client.close()
      backend.close()
    }
  }

  final case class DummyTaggerGraphQLConnector()
      extends TaggerGraphQLConnector
      with LazyLogging {

    // todo JH remove
    def queryEntries(skip: Int, first: Int): Set[EntryView] = {

      throw new RuntimeException(
        s"Query Entries is not implemented in ${getClass.getSimpleName}!"
      )
    }

    def updateEntryWithTags(
        entryUuid: String,
        tagUuids: Set[String]
    ): Option[EntryView] = {

      throw new RuntimeException(
        s"Update entries is not implemented in ${getClass.getSimpleName}!"
      )
    }

    override val authSecret: String = "NO_AUTH_REQUIRED"

    override def close(): Unit = {}
  }

}
