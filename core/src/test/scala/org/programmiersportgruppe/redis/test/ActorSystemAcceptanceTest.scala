package org.programmiersportgruppe.redis.test

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Await, Awaitable, Future, Promise}
import scala.concurrent.duration._
import scala.sys.process.ProcessLogger

import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.ConfigFactory


object ActorSystemAcceptanceTest {
  def addressIfReachable(address: String): Option[InetAddress] =
    Option(InetAddress.getByName(address)).filter(_.isReachable(5000))

  val IPv4LoopbackAddress = addressIfReachable("127.0.0.1")
  val IPv6LoopbackAddress = addressIfReachable("::1")
  val LoopbackAddress = IPv6LoopbackAddress orElse IPv4LoopbackAddress

  val nextRedisServerPort = new AtomicInteger(4321)
}

class ActorSystemAcceptanceTest extends Test {
  import ActorSystemAcceptanceTest._

  implicit val executionContext = scala.concurrent.ExecutionContext.global
  implicit val timeout: Timeout = 5.seconds

  def await(awaitable: Awaitable[_]) = Await.result(awaitable, 5.seconds)

  lazy val redisServerPort = nextRedisServerPort.getAndIncrement

  def withRedisServer[A](testCode: InetSocketAddress => A): A =
    withRedisServer(new InetSocketAddress(LoopbackAddress.get, redisServerPort))(testCode)

  def withRedisServer[A](address: InetSocketAddress)(testCode: InetSocketAddress => A): A = {

    val output = new StringBuilder
    val serverReady = Promise[Unit]()

    val server = sys.process.Process(Seq("redis-server"
      , "--port", address.getPort.toString
      , "--bind", Option(address.getAddress).fold(address.getHostName)(_.getHostAddress.replaceAll("%.*[a-zA-Z].*", ""))
      , "--save", ""  // disable saving state to disk
    )).run(ProcessLogger { line =>
      while(
        try {
          info("Redis server output: " + line.replaceAll("\r?\n?$", ""))
          false
        } catch {
          case e: InterruptedException => true
        }
      ) ()
      output.append(line)
      if (line contains "The server is now ready to accept connections")
        serverReady.success(())
    })
    try {
      serverReady.tryCompleteWith(Future {
        val exitStatus = server.exitValue()
        if (!serverReady.isCompleted)
          throw new RuntimeException("Server terminated unexpectedly with exit status " + exitStatus + ". Output follows:\n" + output)
      })
      await(serverReady.future)
      testCode(address)
    } finally {
      server.destroy()
      server.exitValue()
    }
  }

  def withActorSystem[A](testCode: ActorSystem => A): A = {
    val actorSystem = ActorSystem("Test-actor-system-for-" + getClass.getSimpleName, ConfigFactory.parseString(
      s"""
      akka {
        loglevel = DEBUG
        log-dead-letters = 100
        actor.debug.unhandled = on

        actor.deployment {
          /akre-redis-pool {
            dispatcher = akre-coordinator-dispatcher
          }
          "/akre-redis-pool/*" {
            dispatcher = akre-connection-dispatcher
          }
        }
      }
      akre-coordinator-dispatcher {
        type = PinnedDispatcher
        mailbox-type = "${"org.programmiersportgruppe.redis.client.ResilientPoolMailbox" /*TODO: destrigify again: classOf[ResilientPoolMailbox].getName*/}"
      }
      akre-connection-dispatcher {
        type = PinnedDispatcher
      }
      """
    ))
    try {
      testCode(actorSystem)
    } finally {
      actorSystem.shutdown()
    }
  }

}
