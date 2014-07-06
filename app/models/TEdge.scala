package models

import com.github.aselab.activerecord._
import com.github.aselab.activerecord.dsl._

case class TEdge(src: Int, dist: Int) extends ActiveRecord {
	val tGraphId: Option[Long] = None
	lazy val graph = belongsTo[TGraph]
}

object TEdge extends ActiveRecordCompanion[TEdge]