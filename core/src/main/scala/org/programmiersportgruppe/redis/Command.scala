package org.programmiersportgruppe.redis

import scala.concurrent.Future

import org.programmiersportgruppe.redis.Command._


object Command {

  case class Name(override val toString: String) {
    val asConstants: Seq[Constant] = toString.split(" ").toSeq.map(Constant)
  }

  object Name {
    def fromRValue(value: RValue) = value match {
      case RBulkString(data) => data.map(v => Name(v.decodeString("UTF-8")))
      case _  => None
    }
  }
}

trait Command {
  val name: Name
  val arguments: Seq[CommandArgument]

  def nameAndArguments: Seq[CommandArgument] = name.asConstants ++ arguments
  def asCliString: String = nameAndArguments.mkString(" ")

  def execute(implicit redis: RedisAsync): Future[RSuccessValue] = redis.execute(this)
}


case class UntypedCommand(name: Name, arguments: Seq[CommandArgument]) extends Command {
  override def toString = "UntypedCommand: " + asCliString
}

object UntypedCommand {
  import org.programmiersportgruppe.redis.CommandArgument.ImplicitConversions._

  def apply(name: String, arguments: CommandArgument*): UntypedCommand = new UntypedCommand(Name(name), arguments)

  def fromRValue(arguments: Seq[RValue]) = {
    require(arguments.nonEmpty)
    Name.fromRValue(arguments.head).map { name =>
      new UntypedCommand(name, arguments.tail)
    }
  }

}
