/**
 * © 2021. CoVerified,
 * Diehl, Fetzer, Hiry, Kilian, Mayer, Schlittenbauer, Schweikert, Vollnhals, Weise GbR
 **/

package info.coverified.tagging.service

import info.coverified.tagging.service.TaggingHandler.TaggingResult

object TaggingHandler {

  trait TaggingResult {
    val tags: Vector[String]
  }
}

trait TaggingHandler {

  def analyze(requestString: String): Option[TaggingResult]

}
