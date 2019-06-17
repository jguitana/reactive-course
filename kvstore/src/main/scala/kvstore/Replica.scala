package kvstore

import java.util.UUID

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy}
import kvstore.Arbiter._
import kvstore.PrimaryTransaction.Start

import scala.collection.mutable

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Persistence._
  import Replica._
  import Replicator._

  /**
    * # General setup
    * 1. Create the key value map.
    * 2. Join the cluster and set behavior of primary or secondary.
    * 3. Set up persistence and its restart supervision.
    */

  var kv = mutable.Map.empty[String, String]
  var persistence: ActorRef = null

  override def preStart(): Unit = {
    persistence = context.actorOf(persistenceProps, s"persistence-${UUID.randomUUID()}")
    arbiter ! Join
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(loggingEnabled = false) {
    case _: PersistenceException => Restart
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /**
    * # Leader state and behavior
    *
    * Reading this part requires:
    *   - Understanding PrimaryTransaction.
    *   - Understanding Replicator.
    *
    * 1. Creates the map of replicas to replicators.
    * 2. Creates the buffer to serialize transactions for the same key
    *   and to limit load on this replica.
    * 3. Creates the currently pending keys in transaction.
    * 4. Various utility functions to use for manipulating these data structures.
    * 5. Defines behavior.
    */

  // a map from replica to replicator
  var replicasToReplicator = Map.empty[ActorRef, ActorRef]

  // todo bound buffer and discard new requests past this
  // buffer for transactions by key
  val buffer = mutable.Map.empty[String, mutable.Queue[ActorRef]]

  // keys that are currently in transaction
  val pending = mutable.Set.empty[String]

  /**
    * Utility functions for the buffer map.
    */
  implicit class QueueMap[A](map: mutable.Map[String, mutable.Queue[A]]) {
    def append(k: String, a: A): Unit =
      map.get(k).fold {
        map += (k -> mutable.Queue(a))
        ()
      } { q =>
        q += a
        ()
      }

    def dequeue(k: String): Option[A] =
      map.get(k).fold[Option[A]](None) { queue =>
        if (queue.isEmpty) None
        else Some(queue.dequeue())
      }
  }

  /**
    * Creates a transaction and starts it if no other transaction with the
    * same key is active. This means keys are updated concurrently for different
    * keys and sequentially for same keys. Essentially, serializes modifications
    * for each key.
    */
  // todo concurrent updates to the same key can be allowed if the replicator supports it
  def createTransaction(k: String, v: Option[String], id: Long, sender: ActorRef): Unit = {
    // cannot have named actor with id because it is possible to have multiple when one is terminating
    val transaction = context.actorOf(
      PrimaryTransaction.props(
        k, v, id, sender, replicasToReplicator.values.toSet, persistence))

    if (pending(k)) buffer.append(k, transaction)
    else {
      pending += k
      transaction ! Start
    }

    if (pending.nonEmpty) log.debug("primary pending {}", pending)
    if (buffer.nonEmpty) log.debug("primary buffer {}", buffer)
  }

  /**
    * Pulls the next pending transaction for a given key.
    */
  def nextTransaction(k: String): Unit = {
    buffer.dequeue(k).fold {
      pending -= k
      ()
    }(_ ! Start)
  }

  val leader: Receive = {
    case Insert(k, v, id) =>
      createTransaction(k, Some(v), id, sender)

    case Remove(k, id) =>
      createTransaction(k, None, id, sender)

    case PrimaryTransaction.TransactionAck(k, v, id, sender) =>
      nextTransaction(k)
      v.fold(kv -= k)(vv => kv += (k -> vv))
      sender ! OperationAck(id)

    case PrimaryTransaction.TransactionFail(k, id, sender) =>
      nextTransaction(k)
      sender ! OperationFailed(id)

    case x: Get =>
      sender ! GetResult(x.key, kv.get(x.key), x.id)

    case x: Replicas =>
      val oldReplicas = replicasToReplicator.keys.toSet
      val newReplicas = x.replicas.filter(_ != self)
      val joining = newReplicas diff oldReplicas
      val leaving = oldReplicas diff newReplicas

      // remove replicators for replicas which have left
      replicasToReplicator.foreach {
        case (_replica, replicator) if leaving.contains(_replica) =>
          replicator ! AckAndStop
        case _ =>
      }

      // add replicators for new replicas
      val joiningMap = joining
        .map(r => r -> context.actorOf(Replicator.props(r)))
        .toMap

      // replicate the kv to the new replicas
      joiningMap.foreach {
        case (_replica, replicator) =>
          log.debug("starting internal replication for replica {} with replicator {}", _replica, replicator)
          kv.foreach {
            case (k, v) =>
              replicator ! Replicate(k, Some(v), -1)
          }
      }

      replicasToReplicator = replicasToReplicator -- leaving ++ joiningMap

      log.debug("replicas {}", replicasToReplicator)
  }

  /**
    * Replica
    */

  var ack = 0L
//  var audit: Option[ActorRef] = None

  val replica: Receive = {
    case x: Snapshot if x.seq > ack =>
      log.warning("seq {} > ack {} for snapshot {}", x.seq, ack, x)

    case x: Snapshot if x.seq < ack =>
      log.debug("seq {} < ack {} for snapshot {}", x.seq, ack, x)
//      sender ! SnapshotAck(x.key, x.seq)

      // we can ack here because keys are serialized to the system
    case Snapshot(k, v, seq) =>
      v.fold(kv -= k)(vv => kv += (k -> vv))
      ack += 1
      val _ = context.actorOf(
        SecondaryTransaction.props(
          k, v, seq, sender, persistence), s"secondary-transaction$seq")

    case SecondaryTransaction.TransactionAck(k, _, seq, sender) =>
      sender ! SnapshotAck(k, seq)

    case x: SecondaryTransaction.TransactionFail =>
      log.debug("secondary transaction fail {}", x)

    case x: Get =>
      sender ! GetResult(x.key, kv.get(x.key), x.id)

    case x: Leave =>
      arbiter ! x
  }
}

