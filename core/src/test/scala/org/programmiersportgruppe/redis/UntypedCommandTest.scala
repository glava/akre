package org.programmiersportgruppe.redis

import org.programmiersportgruppe.redis.test.Test
import org.programmiersportgruppe.redis.CommandArgument.ImplicitConversions._


class UntypedCommandTest extends Test {

  behavior of classOf[UntypedCommand].getName

  it should "have a short string representation" in {
    assertResult("""UntypedCommand: SET "zuh?" "yays!" "EX" "7"""") {
      UntypedCommand("SET", Key("zuh?"), "yays!", Constant("EX"), 7).toString
    }
  }

  it should "be able to generate from RValue seq" in {
    assertResult("GET") {
      UntypedCommand
          .fromRValue(Seq(RBulkString("GET"), RBulkString("key")))
          .get.name.toString
    }

    assertResult(StringArgument("key")) {
      UntypedCommand
          .fromRValue(Seq(RBulkString("GET"), RBulkString("key")))
          .get.arguments.head
    }

  }

}
