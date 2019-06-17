package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import kvstore.Persistence.{Persist, Persisted}
import kvstore.PrimaryTransaction.{Start, TransactionAck, TransactionFail}
import kvstore.Replicator.{Replicate, Replicated}

import scala.concurrent.duration._

object PrimaryTransaction {
  case object Start
  case class TransactionAck(key: String, value: Option[String], id: Long, sender: ActorRef)
  case class TransactionFail(key: String, id: Long, sender: ActorRef)
  def props(key: String, value: Option[String], id: Long, sender: ActorRef,
            replicators: Set[ActorRef], persistence: ActorRef) =
    Props(classOf[PrimaryTransaction], key, value, id, sender, replicators, persistence)
}

class PrimaryTransaction(key: String, value: Option[String], id: Long, sender: ActorRef,
                         replicators: Set[ActorRef], persistence: ActorRef) extends Actor with ActorLogging {
  var persistAck = false
  // replicators which did not yet respond
  var replicatorsAck = replicators.size
  var stopTick: Cancellable = Cancellable.alreadyCancelled
  var persistenceTick: Cancellable = Cancellable.alreadyCancelled

  override def preStart(): Unit = {
    stopTick = context.system.scheduler.scheduleOnce(1.second){
      log.debug("primary transaction TIMING OUT")
      context.stop(self)
    }(context.system.dispatcher)
  }

  def acked: Boolean = persistAck && replicatorsAck == 0

  def ackAndTerminate() = if (acked) {
    context.parent ! TransactionAck(key, value, id, sender)
    context.stop(self)
  }

  override def receive: Receive = {
    case Start =>
      log.debug("starting transaction key {} value {} id {}", key, value, id)
      val persist = Persist(key, value, id)

      persistenceTick = context.system.scheduler.schedule(java.time.Duration.ofMillis(0),
        java.time.Duration.ofMillis(100), persistence, persist, context.system.dispatcher, self)

      replicators.foreach(_ ! Replicate(key, value, id))

    case _: Persisted =>
      persistAck = true
      ackAndTerminate()

    case _: Replicated =>
      log.debug("primary transaction acked replicators left {}", replicators.size)
      replicatorsAck -= 1
      ackAndTerminate()
  }

  override def postStop(): Unit = {
    stopTick.cancel()
    persistenceTick.cancel()
    if (!acked) {
      log.error("primary transaction did not ack key {} id {} persistence {} replicators left {}", key, id, persistAck, replicatorsAck)
//      replicatorsAck.foreach(_ ! ReplicateAbort(id))
      context.parent ! TransactionFail(key, id, sender)
    } else {
      log.debug("primary transaction acked! key {} value {} id {}", key, value, id)
    }
  }
}
