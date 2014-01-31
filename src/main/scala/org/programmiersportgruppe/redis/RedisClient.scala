package org.programmiersportgruppe.redis

import java.net.InetSocketAddress
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.{SupervisorStrategy, OneForOneStrategy, ActorSystem}
import akka.routing.RoundRobinPool
import akka.util.{ByteString, Timeout}


case class ErrorReplyException(command: Command, reply: ErrorReply)
  extends Exception(s"Error reply received: ${reply.error}\nFor command: $command\nSent as: ${command.serialised.utf8String}")

class RedisClient(actorSystem: ActorSystem, serverAddress: InetSocketAddress, requestTimeout: Timeout, numberOfConnections: Int, poolName: String = "redis-connection-pool") {
  import akka.pattern.ask
  import actorSystem.dispatcher

  implicit private val timeout = requestTimeout

  private val poolActor = {

    val connection = RedisConnectionActor.props(serverAddress)

    val pool = RoundRobinPool(
      nrOfInstances = 3,
      supervisorStrategy = OneForOneStrategy(3, 5.seconds)(SupervisorStrategy.defaultDecider)
    ).props(connection)

    actorSystem.actorOf(pool, poolName)
  }

  /**
   * Executes a command.
   *
   * @param command the command to be executed
   * @return a non-error reply from the server
   * @throws ErrorReplyException if the server gives an error reply
   * @throws AskTimeoutException if the connection pool fails to respond within the requestTimeout
   */
  def execute(command: Command): Future[ProperReply] = (poolActor ? command).map {
    case (`command`, r: ProperReply) => r
    case (`command`, e: ErrorReply) => throw new ErrorReplyException(command, e)
  }

  /**
   * Executes a command and extracts an optional [[akka.util.ByteString]] from the bulk reply that is expected.
   *
   * @param command the bulk reply command to be executed
   * @throws ErrorReplyException if the server gives an error reply
   * @throws MatchError          if the server gives a proper non-bulk reply
   * @throws AskTimeoutException if the connection pool fails to respond within the requestTimeout
   */
  def executeByteString(command: Command with BulkExpected): Future[Option[ByteString]] =
    execute(command) map { case BulkReply(data) => data }

  /**
   * Executes a command and extracts an optional [[scala.Predef.String]] from the UTF-8 encoded bulk reply that is
   * expected.
   *
   * @param command the bulk reply command to be executed
   * @throws ???                 if the reply cannot be decoded as UTF-8
   * @throws ErrorReplyException if the server gives an error reply
   * @throws MatchError          if the server gives a proper non-bulk reply
   * @throws AskTimeoutException if the connection pool fails to respond within the requestTimeout
   */
  def executeString(command: Command with BulkExpected): Future[Option[String]] =
    execute(command) map { case BulkReply(data) => data.map(_.utf8String) }

  /**
   * Executes a command and extracts a [[scala.Long]] from the int reply that is expected.
   *
   * @param command the int reply command to be executed
   * @throws ErrorReplyException if the server gives an error reply
   * @throws MatchError          if the server gives a proper non-int reply
   * @throws AskTimeoutException if the connection pool fails to respond within the requestTimeout
   */
  def executeLong(command: Command with IntegerExpected): Future[Long] =
    execute(command) map { case IntegerReply(value) => value }

  /**
   * Stops the connection pool used by the client.
   * @throws AskTimeoutException if the connection pool fails to stop within 30 seconds
   */
  def shutdown(): Future[Unit] = {
    akka.pattern.gracefulStop(poolActor, 30.seconds).map(_ => ())
  }

//  def executeBoolean(command: RedisCommand[IntegerReply]): Future[Boolean] = executeAny(command) map { case IntegerReply(0) => false; case IntegerReply(1) => true }
//  def executeBytes(command: RedisCommand[BulkReply]): Future[Option[ByteString]] = executeAny(command) map { case BulkReply(data) => data }
//  def executeString(command: RedisCommand[BulkReply]): Future[Option[String]] = executeAny(command) map { case BulkReply(data) => data.map(_.utf8String) }
}
