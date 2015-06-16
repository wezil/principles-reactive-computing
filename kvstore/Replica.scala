package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

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
  
  case class CheckAck(key: String, id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persistent = context.actorOf(persistenceProps)
  var persistAcks = Map.empty[Long, (ActorRef, Persist)]
  
  var seq = 0L

  case class GlobalAck(
    key: String, 
    id: Long,
    pendingReplicators: Set[ActorRef] = Set(),
    requester: ActorRef,
    persistenceAcked: Boolean = false) {
    val isAcked: Boolean = pendingReplicators.isEmpty && persistenceAcked
  }
  
  object GlobalAck {
    var pending = Map.empty[(String, Long), GlobalAck]
    
    def persistenceAck(key: String, id: Long) = {
      pending.get(key, id).foreach { ack =>
        checkDone(ack.copy(persistenceAcked = true))
        }
    }
    
    def replicateAck(key: String, id: Long, replicator: ActorRef) = {
      pending.get((key, id)).foreach { ack => 
        checkDone(ack.copy(pendingReplicators = ack.pendingReplicators - replicator))}
    }
    
    def registerReplicate(key: String, id: Long, replicator: ActorRef, requester: ActorRef) = {
      val include = pending.getOrElse((key, id), GlobalAck(key = key, id = id, requester = requester))
      pending += Tuple2(key, id) -> include.copy(pendingReplicators = include.pendingReplicators + replicator)
    }
    
    def registerPersist(key: String, id: Long, requester: ActorRef) = {
      val include = pending.getOrElse((key, id), GlobalAck(key = key, id = id, requester = requester))
      pending += Tuple2(key, id) -> include.copy(persistenceAcked = true)
    }
    
    def checkFailed(key: String, id: Long) = {
      pending.get((key, id)).foreach { ack =>
        ack.requester ! OperationFailed(id)
        pending -= Tuple2(key, id)}
    }
    
    def checkDone(ack: GlobalAck) = {
      if (ack.isAcked) {
        ack.requester ! OperationAck(ack.id)
        pending -= Tuple2(ack.key, ack.id)
      }
      else {
        pending += Tuple2(ack.key, ack.id) -> ack
      }
    }
    
    def removeReplicator(replicator: ActorRef) = {
      pending.values.foreach { ack => 
        checkDone(ack.copy(pendingReplicators = ack.pendingReplicators - replicator))}
    }
  }
  
  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    
    //Insert case
    case Insert(key, value, id) => {
      kv += key -> value
      replicate(key, Some(value), id)
    }
    //Remove case
    case Remove(key, id) => {
      kv -= key
      replicate(key, None, id)
    }
    //Get case
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    //case for a new Replicas command, may have to add or remove secondaries & Replicators
    case Replicas(repSet) => {
      val add = repSet -- secondaries.keySet - self
      val remove = secondaries.keySet -- repSet
      
      remove.foreach { r =>
        context.stop(secondaries(r))
        GlobalAck.removeReplicator(secondaries(r))
        secondaries -= r
        }
      
      add.foreach { a =>
        val replicator = context.actorOf(Replicator.props(a))
        secondaries += a -> replicator
        kv.foreach { case (key, value) => 
          replicator ! Replicate(key, Some(value), kv.toList.indexOf(a))
          }
        }
    }
    
    case Persisted(key, id) => {
      GlobalAck.persistenceAck(key, id)
    }
    
    case CheckAck(key, id) => {
      GlobalAck.checkFailed(key, id)
    }
    
    case Replicated(key, id) => {
      GlobalAck.replicateAck(key, id, sender)
    }
  }

  
  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    
    //snapshot case for checking sequence numbers and properly rejecting nonmatching ones
    case Snapshot(key, valueOption, seq) => { 
      if (seq == this.seq) {
        this.seq += 1
        update(key, valueOption)
        persist(key, valueOption, seq)
      }
      else if (seq < this.seq) sender ! SnapshotAck(key, seq)
    }
    
    //Get values from snapshot
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    
    case Persisted(key ,id) => {
      persistAcks(id)._1 ! SnapshotAck(key, id)
      persistAcks -= id
    }
  }
  
  def replicate(key: String, valueOption: Option[String], id: Long) = {
    secondaries.values.foreach { replicator => 
      GlobalAck.registerReplicate(key, id, replicator, sender)
      replicator ! Replicate(key, valueOption, id)
      }
    GlobalAck.registerPersist(key, id, sender)
    persist(key, valueOption, id)
    context.system.scheduler.scheduleOnce(1 second, self, CheckAck(key, id))
  }
  
  def update(key: String, valueOption: Option[String]) = {
    if (valueOption.isDefined) kv += key -> valueOption.get
    else kv -= key
  }
  
  def persist(key: String, valueOption: Option[String], seq: Long) = {
    val p = Persist(key, valueOption, seq)
    persistAcks += seq -> (sender, p)
    persistent ! p
    
  }
  
  def repersist() = {
    persistAcks.foreach { case (id, (_, p)) => persistent ! p 
      }
  }

  override def preStart() = {
    arbiter ! Join
    context.system.scheduler.schedule(0 milliseconds, 100 milliseconds) (repersist)
  }
}

