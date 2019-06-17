package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import kvstore.Replicator._

import scala.concurrent.duration._

object ReplicatorTransaction {
  def props(timeout: FiniteDuration, tick: FiniteDuration, seq: Long, replicate: Replicate, replica: ActorRef, sender: ActorRef) =
    Props(classOf[ReplicatorTransaction], timeout, tick, seq, replicate, replica, sender)
}

class ReplicatorTransaction(timeout: FiniteDuration, tick: FiniteDuration, seq: Long, replicate: Replicate, replica: ActorRef, sender: ActorRef) extends Actor with ActorLogging {
  var stopTick: Cancellable = Cancellable.alreadyCancelled
  var snapshotTick: Cancellable = Cancellable.alreadyCancelled
  var acked = false

  override def preStart(): Unit = {
    implicit val ec = context.system.dispatcher

    val snapshot = Snapshot(replicate.key, replicate.valueOption, seq)
    snapshotTick = context.system.scheduler.schedule(0.millis,
      tick, replica, snapshot)
    stopTick = context.system.scheduler.scheduleOnce(1.second){
      log.debug("replicator transaction TIMING OUT")
      context.stop(self)
    }(context.system.dispatcher)
  }


  override def receive: Receive = {
    case x: SnapshotAck =>
      log.debug("snapshot ack {} replicator {}", x, context.parent)
      acked = true
      sender ! Replicated(replicate.key, replicate.id)
      context.parent ! x
      context.stop(self)

    case AckAndStop =>
      sender ! Replicated(replicate.key, replicate.id)
      context.stop(self)
  }

  override def postStop(): Unit = {
    if (acked) {
      log.debug("stopping ACKED replicator transaction seq {}", seq)
    }
    else {
      log.debug("stopping UN-ACKED replicator transaction seq {}", seq)
    }
    snapshotTick.cancel()
    stopTick.cancel()
  }
}
