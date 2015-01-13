package org.programmiersportgruppe.redis.fake

import akka.util.ByteString

import org.programmiersportgruppe.redis._
import org.programmiersportgruppe.redis.commands._
import org.programmiersportgruppe.redis.protocol.RValueParser


class InMemoryRedisFake {

  var db: Map[Key, ByteString] = Map.empty

  def execute(command: RecognisedCommand): RValue = command match {
    case GET(key) => RBulkString(db(key))
    case SET(key, value, _, _) =>
      db = db.updated(key, value)
      RSimpleString.OK
    case c => RError("I have no clue what this command is! " + c)
  }

}
