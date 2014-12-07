package org.programmiersportgruppe.redis

import akka.util.ByteString


sealed abstract class CommandArgument {
  def asByteString: ByteString
}

object CommandArgument {

  object ImplicitConversions {

    import scala.language.implicitConversions

    implicit def byteString2StringArgument(s: ByteString): StringArgument = StringArgument(s)
    implicit def string2StringArgument(s: String): StringArgument = StringArgument(s)
    implicit def long2IntegerArgument(l: Long): IntegerArgument = IntegerArgument(l)
    implicit def rString2StringArgument(s: RSimpleString): StringArgument = StringArgument(s.value)

    implicit def commandArgument2Key(c: CommandArgument): Key = Key(c.asByteString)

    implicit def seqRValue2SeqCommandArgument(s: Seq[RValue]): Seq[CommandArgument] = s.map {
      case RBulkString(Some(data)) => StringArgument(data)
      case _ => throw new IllegalArgumentException("Currently don't support this argument type ")
    }
  }

}


case class Constant(override val toString: String) extends CommandArgument {
  override def asByteString = toString
}


case class StringArgument(asByteString: ByteString) extends CommandArgument {
  override def toString = s"<bytes=${asByteString.size}>"
}

object StringArgument {
  def apply(value: String): StringArgument = new StringArgument(value)
}


case class Key(asByteString: ByteString) extends CommandArgument {
  override def toString = asByteString.utf8String
}


case class IntegerArgument(value: Long) extends CommandArgument {
  override def asByteString = value.toString
  override def toString = value.toString
}


case class DoubleArgument(value: Double) extends CommandArgument {
  override def asByteString = value.toString
  override def toString = value.toString
}
