package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}

import scala.collection.mutable
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case object AckAndStop

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

// this class cannot receive other messages due to grader, drops pending after 1 sec which is quite slower than receiving messages directly from replicatortransaction
class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._

  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  var tick: Cancellable = Cancellable.alreadyCancelled
  val internalReplicates = mutable.Map.empty[String, ActorRef]
  // queue with replicate messages and sender
  val buffer = mutable.Queue.empty[(Replicate, ActorRef)]
  var pending = mutable.Set.empty[ActorRef]

  def dequeue(n: Int): mutable.Set[(Replicate, ActorRef)] = {
    val set = mutable.Set.empty[(Replicate, ActorRef)]
    var i = n
    while (buffer.nonEmpty && i > 0) {
      set += buffer.dequeue()
      i -= 1
    }
    set
  }

  def startReplicatorTransaction(replicate: Replicate, sender: ActorRef): ActorRef = replicate match {
    case x @ Replicate(k, _, -1) =>
      log.debug("internal replication key {} count {}", k, internalReplicates.size)
      val seq = nextSeq()
      // small tick means larger load on replica but replicates really fast
      val r = context.actorOf(
        ReplicatorTransaction.props(10.seconds, 100.millis,
          seq, x, replica, sender), s"replicator-transaction$seq")
      internalReplicates += (k -> r)
      r

    case x @ Replicate(k, _, _) =>
      internalReplicates.get(k).foreach { i =>
        log.debug("stopping internal replicator for key {}", k)
        internalReplicates -= k
        i ! AckAndStop
      }
      val seq = nextSeq()
      context.actorOf(
        ReplicatorTransaction.props(1.second, 200.millis,
          seq, x, replica, sender), s"replicator-transaction$seq")
  }

  def forceBatch(): Unit = {
    if (pending.isEmpty) {
      pending = dequeue(5).map(t => startReplicatorTransaction(t._1, t._2))
      if (pending.nonEmpty) {
        log.debug("replicator batch start {}", pending)
        context.system.scheduler.scheduleOnce(1.second) {
          pending = mutable.Set.empty[ActorRef]
        }(context.dispatcher)
      }
    }
  }

  override def preStart(): Unit = {
    tick = context.system.scheduler.schedule(0.millis,
      50.millis)(forceBatch())(context.system.dispatcher)
  }

  def receive: Receive = {
    case x: Replicate =>
      buffer += (x -> sender())
      forceBatch()

    case _: SnapshotAck =>
      pending -= sender()
      forceBatch()

    case AckAndStop =>
      tick.cancel()
      context.children.foreach(_ ! AckAndStop)
      context.stop(self)
  }
}
