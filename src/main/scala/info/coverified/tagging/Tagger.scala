/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging

import com.typesafe.scalalogging.LazyLogging
import info.coverified.graphql.Connector

import info.coverified.graphql.schema.CoVerifiedClientSchema.CloudinaryImage_File.CloudinaryImage_FileView
import info.coverified.graphql.schema.CoVerifiedClientSchema.Language.LanguageView
import info.coverified.graphql.schema.CoVerifiedClientSchema.Tag.TagView
import info.coverified.graphql.schema.CoVerifiedClientSchema.{
  CloudinaryImage_File,
  Entry,
  EntryUpdateInput,
  EntryWhereInput,
  GeoLocation,
  Language,
  LocationGoogle,
  Mutation,
  Query,
  Source,
  Tag,
  TagCreateInput,
  TagRelateToManyInput,
  TagWhereUniqueInput,
  _QueryMeta
}
import info.coverified.tagging.service.{TaggingHandler, Watson}
import sttp.client3.asynchttpclient.zio.SttpClient
import sttp.model.Uri
import zio.{RIO, Task, ZIO}
import zio.console.Console

final case class Tagger(apiUrl: Uri, handler: TaggingHandler)
    extends LazyLogging {

  import Tagger._

  def queryUntaggedEntries
      : ZIO[Console with SttpClient, Throwable, Iterable[EntryView]] =
    Connector
      .sendRequest(untaggedEntriesQuery.toRequest(apiUrl))
      .map(x => x.getOrElse(Iterable.empty).flatten)

  def queryExistingTags: ZIO[Console with SttpClient, Throwable, Iterable[
    TagView[LanguageView, CloudinaryImage_FileView]
  ]] =
    Connector
      .sendRequest(existingTagsQuery.toRequest(apiUrl))
      .map(x => x.getOrElse(Iterable.empty).flatten)

  def updateTags(
      untaggedEntries: Iterable[EntryView],
      existingTags: Iterable[TagView[LanguageView, CloudinaryImage_FileView]]
  ): ZIO[
    Console with SttpClient,
    Throwable,
    (
        Set[Option[TagView[LanguageView, CloudinaryImage_FileView]]],
        Seq[EntryWithTags]
    )
  ] = {
    val deriveTags = untaggedEntries
      .flatMap(untaggedEntry => {
        untaggedEntry.content.map(
          entryContent =>
            Task {
              val entryTags: Vector[String] = handler
                .analyze(entryContent)
                .flatMap {
                  case watson @ Watson.WatsonTaggingResult(_) =>
                    Some(watson.tags)
                  case unknown =>
                    logger.error(
                      s"Unknown tagging response $unknown. Will be ignored!"
                    )
                    None
                }
                .getOrElse(Vector.empty)
              EntryWithTags(untaggedEntry, entryTags)
            }
        )
      })
      .toSeq

    val createTagInDb: String => RIO[
      zio.console.Console with sttp.client3.asynchttpclient.zio.SttpClient,
      Option[TagView[LanguageView, CloudinaryImage_FileView]]
    ] = (tag: String) =>
      Connector.sendRequest(
        Mutation
          .createTag(Some(TagCreateInput(name = Some(tag))))(
            Tag.view(Language.view, CloudinaryImage_File.view())
          )
          .toRequest(apiUrl)
      )

    val filterExistingTags: Seq[EntryWithTags] => Set[String] =
      (entriesWithTags: Seq[EntryWithTags]) =>
        entriesWithTags
          .flatMap(_.tags)
          .toSet
          .filterNot(existingTags.flatMap(_.name).toSet)

    ZIO
      .collectAllPar(deriveTags)
      .flatMap(entriesWithTags => {
        ZIO
          .collectAllPar(filterExistingTags(entriesWithTags).map(createTagInDb))
          .zip(ZIO.succeed(entriesWithTags))
      })
  }

  def tagEntries(
      allTags: Map[String, TagView[LanguageView, CloudinaryImage_FileView]],
      entriesWithTags: Seq[EntryWithTags]
  ): ZIO[Console with SttpClient, Throwable, Seq[Option[EntryView]]] = {

    ZIO.collectAllPar(entriesWithTags.map(entryWithTags => {
      val tags = entryWithTags.tags
        .flatMap(allTags.get)
        .map(tagView => Some(TagWhereUniqueInput(tagView.id)))
        .toList
      Connector.sendRequest(
        updateEntryWithTags(entryWithTags.entry.id, tags)
          .toRequest(apiUrl)
      )
    }))
  }

}

object Tagger {

  type EntryView = Entry.EntryView[
    CloudinaryImage_FileView,
    TagView[LanguageView, CloudinaryImage_FileView],
    _QueryMeta._QueryMetaView,
    LanguageView,
    Source.SourceView[
      GeoLocation.GeoLocationView[LocationGoogle.LocationGoogleView]
    ]
  ]

  final case class EntryWithTags(
      entry: EntryView,
      tags: Vector[String]
  )

  private val fullEntryViewInnerSelection = Entry.view()(
    CloudinaryImage_File.view(),
    Tag.view(Language.view, CloudinaryImage_File.view()),
    _QueryMeta.view,
    Language.view,
    Source.view(GeoLocation.view(LocationGoogle.view))
  )

  private val untaggedEntriesQuery =
    Query.allEntries(
      where = Some(
        EntryWhereInput(
          hasBeenTagged = Some(false)
        )
      )
    )(fullEntryViewInnerSelection)

  private val existingTagsQuery =
    Query.allTags()(Tag.view(Language.view, CloudinaryImage_File.view()))

  private val updateEntryWithTags =
    (entryId: String, tags: List[Option[TagWhereUniqueInput]]) =>
      Mutation
        .updateEntry(
          entryId,
          Some(
            EntryUpdateInput(
              hasBeenTagged = Some(true),
              tags = Some(
                TagRelateToManyInput(
                  connect = Some(
                    tags
                  )
                )
              )
            )
          )
        )(fullEntryViewInnerSelection)

}
