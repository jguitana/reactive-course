/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import akka.actor._

import scala.collection.mutable

object BinaryTreeSet {
  def insert(x: Insert, opt: Option[ActorRef], context: ActorContext): Option[ActorRef] =
    opt.fold[Option[ActorRef]] {
      x.requester ! OperationFinished(x.id)
      Some(context.actorOf(BinaryTreeNode.props(x.elem)))
    } { actor =>
      actor ! x
      opt
    }

  def contains(x: Contains, opt: Option[ActorRef]): Unit =
    opt.fold {
      x.requester ! ContainsResult(x.id, result = false)
    }(_ ! x)

  def remove(x: Remove, opt: Option[ActorRef]): Unit =
    opt.fold{
      x.requester ! OperationFinished(x.id)
    }(_ ! x)

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  case class GCCollected(set: Seq[Int])

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

class BinaryTreeSet extends Actor {

  import BinaryTreeSet._

  var root: Option[ActorRef] = None

  var pendingQueue = mutable.Queue.empty[Operation]

  def receive = normal


  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case x: Insert =>
      root = insert(x, root, context)

    case x: Contains =>
      contains(x, root)

    case x: Remove =>
      remove(x, root)

    case GC =>
      root.foreach { r =>
        r ! GC
        context.become(garbageCollecting)
      }
  }

  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting: Receive = {
    case msg: Operation =>
      pendingQueue enqueue msg

    case GCCollected(elems) =>
      root.foreach(_ ! PoisonPill)
      root = None

      elems.foreach { e =>
        root = insert(Insert(context.parent, 0, e), root, context)
      }

      pendingQueue.iterator.foreach {
        case x: Insert => root = insert(x, root, context)
        case x: Contains => contains(x, root)
        case x: Remove => remove(x, root)
      }

      pendingQueue = mutable.Queue.empty[Operation]

      context.become(normal)
  }
}

object BinaryTreeNode {
  def props(elem: Int) = Props(classOf[BinaryTreeNode], elem)
}

class BinaryTreeNode(val elem: Int) extends Actor {

  import BinaryTreeSet._

  var left: Option[ActorRef] = None
  var right: Option[ActorRef] = None
  var removed = false
  var gcCount = 0
  var gcElems = List.empty[Int]

  def receive = normal

  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, el) if el == elem =>
      removed = false
      requester ! OperationFinished(id)

    case x @ Insert(_, _, el) if el < elem =>
      left = insert(x, left, context)

    case x @ Insert(_, _, el) if el > elem =>
      right = insert(x, right, context)

    case Contains(requester, id, el) if el == elem && !removed =>
      requester ! ContainsResult(id, result = true)

    case Contains(requester, id, el) if el == elem =>
      requester ! ContainsResult(id, result = false)

    case x @ Contains(_, _, el) if el < elem =>
      contains(x, left)

    case x @ Contains(_, _, el) if el > elem =>
      contains(x, right)

    case Remove(requester, id, el) if el == elem =>
      removed = true
      requester ! OperationFinished(id)

    case x @ Remove(_, _, el) if el < elem =>
      remove(x, left)

    case x @ Remove(_, _, el) if el > elem =>
      remove(x, right)

    case GC =>
      if (!removed) gcElems = List(elem)
      left.fold(gcCount += 1)(_ ! GC)
      right.fold(gcCount += 1)(_ ! GC)
      if (gcCount == 2) context.parent ! GCCollected(gcElems)

    case GCCollected(elems) =>
      gcCount += 1
      gcElems = gcElems ++ elems
      if (gcCount == 2) context.parent ! GCCollected(gcElems)
  }
}
