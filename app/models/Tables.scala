package models

import com.github.aselab.activerecord._
import com.github.aselab.activerecord.dsl._

object Tables extends ActiveRecordTables {
  val edges = table[TEdge]
  val graph = table[TGraph]
}