package org.programmiersportgruppe.redis.fake

import java.net.InetSocketAddress

import akka.actor.{Props, Actor}
import akka.io.{IO, Tcp}
import org.programmiersportgruppe.redis.commands.RecognisedCommand
import org.programmiersportgruppe.redis.{UntypedCommand, RArray}
import org.programmiersportgruppe.redis.protocol.RValueParser


object RedisServerFake {
    def props(): Props = Props(classOf[RedisServerFake])
}

class RedisServerFake extends Actor {

    import Tcp._
    import context.system
    //TODO: please dont bound this like this
    IO(Tcp) ! Bind(self, new InetSocketAddress("127.0.0.1", 4321))

    def receive = {
        case b @ Bound(localAddress) =>
        // do some logging or setup ...
            println("bound")

        case CommandFailed(_: Bind) => context stop self

        case c @ Connected(remote, local) =>
            val handler = context.actorOf(Props[CommandHandler])
            val connection = sender()
            connection ! Register(handler)
    }

}

class CommandHandler extends Actor {
    import Tcp._
    //TODO: this is not correct place for this
    val inMemoryRedis = new InMemoryRedisFake
    def receive = {
        case Received(data) => {
            RValueParser.parseNonEmpty(data).map {
                    case RArray(cont) => UntypedCommand.fromRValue(cont).flatMap(execute)
                    case _ => None
            }.map { result =>
                sender() ! Write(result.get)
            }

        }
        case PeerClosed     => context stop self
    }

    private def execute(uc: UntypedCommand) = {
        RecognisedCommand.fromUntypedCommand(uc).map(inMemoryRedis.execute)
    }

}
