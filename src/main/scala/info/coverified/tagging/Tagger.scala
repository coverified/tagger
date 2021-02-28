/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging

import com.typesafe.scalalogging.LazyLogging
import info.coverified.graphql.Connector
import info.coverified.graphql.schema.CoVerifiedClientSchema.CloudinaryImage_File.CloudinaryImage_FileView
import info.coverified.graphql.schema.CoVerifiedClientSchema.Entry.EntryView
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
import info.coverified.tagging.Tagger.EntryWithTags
import info.coverified.tagging.service.{TaggingHandler, Watson}
import sttp.client3.asynchttpclient.zio.SttpClient
import sttp.model.Uri
import zio.{RIO, Task, ZIO}
import zio.console.Console

final case class Tagger(apiUrl: Uri,  handler: TaggingHandler) extends LazyLogging {

  private val untaggedEntriesQuery =
    Query.allEntries(
      where = Some(
        EntryWhereInput(
          hasBeenTagged = Some(false)
        )
      )
    )(
      Entry.view()(
        CloudinaryImage_File.view(),
        Tag.view(Language.view, CloudinaryImage_File.view()),
        _QueryMeta.view,
        Language.view,
        Source.view(GeoLocation.view(LocationGoogle.view))
      )
    )

  private val existingTagsQuery =
    Query.allTags()(Tag.view(Language.view, CloudinaryImage_File.view()))

  def queryUntaggedEntries
      : ZIO[Console with SttpClient, Throwable, Iterable[EntryView[
        CloudinaryImage_FileView,
        TagView[LanguageView, CloudinaryImage_FileView],
        _QueryMeta._QueryMetaView,
        LanguageView,
        Source.SourceView[
          GeoLocation.GeoLocationView[LocationGoogle.LocationGoogleView]
        ]
      ]]] =
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
      untaggedEntries: Iterable[EntryView[
        CloudinaryImage_FileView,
        TagView[LanguageView, CloudinaryImage_FileView],
        _QueryMeta._QueryMetaView,
        LanguageView,
        Source.SourceView[
          GeoLocation.GeoLocationView[LocationGoogle.LocationGoogleView]
        ]
      ]],
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

    ZIO
      .collectAllPar(deriveTags)
      .flatMap(entriesWithTags => {
        val uniqueNewTags: Set[String] = entriesWithTags
          .flatMap(_.tags)
          .toSet
          .filterNot(existingTags.flatMap(_.name).toSet)
        ZIO
          .collectAllPar(uniqueNewTags.map(createTagInDb))
          .zip(ZIO.succeed(entriesWithTags))
      })
  }

  def tagEntries(
      allTags: Map[String, TagView[LanguageView, CloudinaryImage_FileView]],
      entriesWithTags: Seq[EntryWithTags]
  ): ZIO[Console with SttpClient, Throwable, Seq[Option[EntryView[
    CloudinaryImage_FileView,
    TagView[LanguageView, CloudinaryImage_FileView],
    _QueryMeta._QueryMetaView,
    LanguageView,
    Source.SourceView[
      GeoLocation.GeoLocationView[LocationGoogle.LocationGoogleView]
    ]
  ]]]] = {

    ZIO.collectAllPar(entriesWithTags.map(entryWithTags => {
      val tags = entryWithTags.tags
        .flatMap(allTags.get)
        .map(tagView => Some(TagWhereUniqueInput(tagView.id)))
        .toList
      Connector.sendRequest(
        Mutation
          .updateEntry(
            entryWithTags.entry.id,
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
          )(
            Entry.view()(
              CloudinaryImage_File.view(),
              Tag.view(Language.view, CloudinaryImage_File.view()),
              _QueryMeta.view,
              Language.view,
              Source.view(GeoLocation.view(LocationGoogle.view))
            )
          )
          .toRequest(apiUrl)
      )
    }))
  }

}

object Tagger {

  final case class EntryWithTags(
      entry: EntryView[
        CloudinaryImage_FileView,
        TagView[LanguageView, CloudinaryImage_FileView],
        _QueryMeta._QueryMetaView,
        LanguageView,
        Source.SourceView[
          GeoLocation.GeoLocationView[LocationGoogle.LocationGoogleView]
        ]
      ],
      tags: Vector[String]
  )

}
