package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case rep @ Replicate(key, valueOption, id) => {
      val seq = nextSeq
      acks += seq -> (sender, rep)
      send(rep, seq)
    }
    case SnapshotAck(key, seq) => {
      acks.get(seq).foreach { case(requester, r) => 
        requester ! Replicated(key, r.id)
        acks -= seq
        }
    }
  }
  
  def send(r: Replicate, seq: Long) = {
    replica ! Snapshot(r.key, r.valueOption, seq)
  }
  
  def resend() = {
    acks.foreach { case (seq, (_, r)) => 
      send(r, seq)     
    }
  }

  override def preStart() = {
    context.system.scheduler.schedule(0 milliseconds, 100 milliseconds)(resend)
  }
}
