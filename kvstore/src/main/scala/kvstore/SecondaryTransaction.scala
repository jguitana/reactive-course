package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import kvstore.Persistence.{Persist, Persisted}
import kvstore.SecondaryTransaction.{TransactionAck, TransactionFail}

import scala.concurrent.duration._

object SecondaryTransaction {

  case class TransactionAck(key: String, value: Option[String], seq: Long, sender: ActorRef)
  case class TransactionFail(seq: Long, sender: ActorRef)

  def props(key: String, value: Option[String], seq: Long, sender: ActorRef, persistence: ActorRef) =
    Props(classOf[SecondaryTransaction], key, value, seq, sender, persistence)
}

class SecondaryTransaction(key: String, value: Option[String], seq: Long, sender: ActorRef, persistence: ActorRef) extends Actor with ActorLogging {
  var stopTick: Cancellable = Cancellable.alreadyCancelled
  var persistenceTick: Cancellable = Cancellable.alreadyCancelled
  var persisted: Boolean = false

  override def preStart(): Unit = {
    implicit val ec = context.system.dispatcher

    val persist = Persist(key, value, seq)
    persistenceTick = context.system.scheduler.schedule(0.millis,
      100.millis, persistence, persist)
    stopTick = context.system.scheduler.scheduleOnce(1.second){
      log.debug("secondary transaction TIMING OUT")
      context.stop(self)
    }(context.system.dispatcher)
  }

  override def receive: Receive = {
    case _: Persisted =>
      persisted = true
      context.parent ! TransactionAck(key, value, seq, sender)
      context.stop(self)
  }

  override def postStop(): Unit = {
    if (!persisted) context.parent ! TransactionFail(seq, sender)
    persistenceTick.cancel()
    stopTick.cancel()
  }
}
