package models

import com.github.aselab.activerecord._
import com.github.aselab.activerecord.dsl._

case class TGraph(name:String) extends ActiveRecord {
	lazy val edges = hasMany[TEdge]
}

object TGraph extends ActiveRecordCompanion[TGraph]