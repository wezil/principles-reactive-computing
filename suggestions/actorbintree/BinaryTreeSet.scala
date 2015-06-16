/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

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

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = { 
    case message: Operation => root ! message
    case GC => {
      val newTree = createRoot
      context.become(garbageCollecting(newTree))
      root ! CopyTo(newTree)
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC => None
    
    case message: Operation => pendingQueue = pendingQueue.enqueue(message)
    
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot
      
      while (!pendingQueue.isEmpty) {
        val (message, subQueue) = pendingQueue.dequeue
        pendingQueue = subQueue
        root ! message
      }
     
      context.become(normal)
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = { 
    
    //Insert Operation
    case message @ Insert(sender, id, elem) => {
      val done = OperationFinished(id)
      if (this.elem == elem) {
        this.removed = false
        sender ! done
      }
      else if (this.elem < elem) {
        subtrees.get(Right) match {
          case Some(right) => right ! message
          case None => {
            subtrees = subtrees.updated(Right, context.actorOf(BinaryTreeNode.props(elem, false)))
            sender ! done
          }
        }
      }
      else if (this.elem > elem) {
        subtrees.get(Left) match {
          case Some(left) => left ! message
          case None => {
            subtrees = subtrees.updated(Left, context.actorOf(BinaryTreeNode.props(elem, false)))
            sender ! done
          }
        }
      }
    }
    
    //Remove Operation
    case message @ Remove(sender, id, elem) => {
      val done = OperationFinished(id)
      if (this.elem == elem) {
        this.removed = true
        sender ! done
      }
      else if (this.elem < elem) {
        subtrees.get(Right) match {
          case Some(right) => right ! message
          case None => sender ! done
        }
      }
      else if (this.elem > elem) {
        subtrees.get(Left) match {
          case Some(left) => left ! message
          case None => sender ! done
        }
      }
     
    }
    
    //Contains Operation
    case message @ Contains(sender, id, elem) => {
      if (this.elem == elem) {
        sender ! ContainsResult(id, !this.removed)
      }
      else if (this.elem < elem) {
        subtrees.get(Right) match {
          case Some(right) => right ! message
          case None => sender ! ContainsResult(id, false)
        }
      }
      else if (this.elem > elem) {
        subtrees.get(Left) match {
          case Some(left) => left ! message
          case None => sender ! ContainsResult(id, false)
        }
      }
    }
    
    //CopyTo Operation
    case CopyTo(treeNode) => {
      val children = subtrees.values.toSet
      if (children.isEmpty && removed) context.parent ! CopyFinished
      else {
        for (c <- children) c ! CopyTo(treeNode)
        context.become(copying(children, removed))
        
        if (!removed) treeNode ! Insert(self, elem, elem)
        else self ! OperationFinished(elem)
      }  
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) => {
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context.become(normal)
      }
      else context.become(copying(expected, true))
    }
    
    case CopyFinished =>
      val expectedSub = expected - sender
      if (expectedSub.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.become(normal)
      }
      else context.become(copying(expectedSub, insertConfirmed))
  }
}
