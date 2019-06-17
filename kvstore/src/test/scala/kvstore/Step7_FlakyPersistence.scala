package kvstore

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.testkit.TestProbe
import org.scalatest.{FunSuiteLike, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

// todo remove thread sleeps
// todo OperationFail is expected to happen under load, tests should not fail for few of these
trait Step7_FlakyPersistence extends FunSuiteLike
  with Matchers { this: KVStoreSuite =>
  import Arbiter._

  test("Step7-case1: Three replica cluster should be resilient under flaky persistence and lossy communication") {
    val testProbe = TestProbe()
    val arbiter = system.actorOf(kvstore.given.Arbiter.props(lossy = true, testProbe.testActor))
    val primary = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case1-primary")

    testProbe.expectMsg(JoinedPrimary)

    val secondary1 = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case1-secondary1")

    testProbe.expectMsg(JoinedSecondary)

    val secondary2 = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case1-secondary2")

    testProbe.expectMsg(JoinedSecondary)

    val client = session(primary)
    val client1 = session(secondary1)
    val client2 = session(secondary2)

    // 1. primary sets and acks an operation
    // 2. secondary is able to get all previously acked operations
    // 3. same as above for primary
    // 4. repeats 1,2,3 for removal
    for (i <- 0 to 15) yield {
      client.set(s"k$i", i.toString, i)
      client.waitAck(i)
      client1.get(s"k$i").getOrElse(throw new Exception(s"bad insert secondary1 k$i"))
      client2.get(s"k$i").getOrElse(throw new Exception(s"bad insert secondary2 k$i"))
      client.get(s"k$i").getOrElse(throw new Exception(s"bad insert primary k$i"))
      client.remove(s"k$i", i)
      client.waitAck(i)
      client1.get(s"k$i").foreach(s => throw new Exception(s"bad remove secondary1 got $s"))
      client2.get(s"k$i").foreach(s => throw new Exception(s"bad remove secondary2 got $s"))
      client.get(s"k$i").foreach(s => throw new Exception(s"bad remove primary got $s"))
    }
  }

  test("Step7-case2: Replicas kv store will eventually converge with the primary") {
    val testProbe = TestProbe()
    val arbiter = system.actorOf(kvstore.given.Arbiter.props(lossy = true, testProbe.testActor))
    val primary = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case2-primary")

    testProbe.expectMsg(JoinedPrimary)
    val client = session(primary)

    val num = 15

    for (i <- 0 to num) yield {
      client.set(s"k$i", i.toString, i)
      client.waitAck(i)
    }

    val secondary1 = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case2-secondary1")
//    val audit = TestProbe()

    testProbe.expectMsg(JoinedSecondary)
    val client1 = session(secondary1)

    Thread.sleep(2000)

    for (i <- 0 to num) yield
      client1.get(s"k$i").getOrElse(throw new Exception(s"bad insert secondary1 k$i"))

    val secondary2 = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case2-secondary2")

    testProbe.expectMsg(JoinedSecondary)
    val client2 = session(secondary2)

    Thread.sleep(2000)

    for (i <- 0 to num) yield
      client2.get(s"k$i").getOrElse(throw new Exception(s"bad insert secondary2 k$i"))
  }

  // multiple concurrent sets may timeout because they must finish both within 1 sec
  test("Step7-case3: A key should be updated sequentially") {
    val testProbe = TestProbe()
    val arbiter = system.actorOf(kvstore.given.Arbiter.props(lossy = true, testProbe.testActor))
    val primary = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case3-primary")

    testProbe.expectMsg(JoinedPrimary)

    val secondary1 = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case3-secondary1")

    testProbe.expectMsg(JoinedSecondary)

    val secondary2 = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case3-secondary2")

    testProbe.expectMsg(JoinedSecondary)

    val client = session(primary)
    val client1 = session(secondary1)
    val client2 = session(secondary2)

    val nextId = Iterator.from(0)

    for (i <- 0 to 15) yield {
      val v1 = nextId.next()
//      val v2 = nextId.next()
//      val v3 = nextId.next()
//      val v4 = nextId.next()
      val last = nextId.next()
      client.set(s"k$i", v1.toString, v1)
//      client.set(s"k$i", v2.toString, v2)
//      client.set(s"k$i", v3.toString, v3)
//      client.set(s"k$i", v4.toString, v4)
      client.set(s"k$i", last.toString, last)
      client.waitAck(v1)
//      client.waitAck(v2)
//      client.waitAck(v3)
//      client.waitAck(v4)
      client.waitAck(last)
      client1.get(s"k$i").flatMap(s => if (s != last.toString) None else Some(s)).getOrElse(throw new Exception(s"bad insert secondary1 k$i"))
      client2.get(s"k$i").flatMap(s => if (s != last.toString) None else Some(s)).getOrElse(throw new Exception(s"bad insert secondary2 k$i"))
      client.get(s"k$i").flatMap(s => if (s != last.toString) None else Some(s)).getOrElse(throw new Exception(s"bad insert primary k$i"))
    }
  }

  test("Step7-case4: Multiple keys can be updated in parallel") {
    implicit val ec = scala.concurrent.ExecutionContext.Implicits.global
    val testProbe = TestProbe()
    val arbiter = system.actorOf(kvstore.given.Arbiter.props(lossy = true, testProbe.testActor))
    val primary = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case4-primary")

    testProbe.expectMsg(JoinedPrimary)

    val secondary1 = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case4-secondary1")

    testProbe.expectMsg(JoinedSecondary)

    val secondary2 = system.actorOf(Replica.props(arbiter, Persistence.props(true)), "step7-case4-secondary2")

    testProbe.expectMsg(JoinedSecondary)

    def perKey(i: Int) = Future {
      val client = session(primary)
      val client1 = session(secondary1)
      val client2 = session(secondary2)
      client.set(s"k$i", i.toString, i)
      client.waitAck(i)
      client1.get(s"k$i").getOrElse(throw new Exception(s"bad insert secondary1 k$i"))
      client2.get(s"k$i").getOrElse(throw new Exception(s"bad insert secondary2 k$i"))
      client.get(s"k$i").getOrElse(throw new Exception(s"bad insert primary k$i"))
      client.remove(s"k$i", i)
      client.waitAck(i)
      client1.get(s"k$i").foreach(s => throw new Exception(s"bad remove secondary1 got $s"))
      client2.get(s"k$i").foreach(s => throw new Exception(s"bad remove secondary2 got $s"))
      client.get(s"k$i").foreach(s => throw new Exception(s"bad remove primary got $s"))
    }

    Await.result(Future.sequence((0 to 15).map(perKey).toList), Duration(20, TimeUnit.SECONDS))
  }

  test("Step7-case5: Random inputs") {
    implicit val ec = scala.concurrent.ExecutionContext.Implicits.global
    val arbiterAudit = TestProbe()
    val arbiter = system.actorOf(kvstore.given.Arbiter.props(lossy = true, arbiterAudit.testActor))
    val primary = system.actorOf(Replica.props(arbiter, Persistence.props(true)))
    arbiterAudit.expectMsg(JoinedPrimary)

    // can have concurrent access but wtver
    var maxReplicas = 0
    var currentReplicas = Set.empty[ActorRef]

    // replica and session
    def join(force: Boolean) = {
      if (force || Random.nextInt(100) < 5) {
        val a = system.actorOf(Replica.props(arbiter, Persistence.props(true)))
        println(s"JOINING $a")
        currentReplicas = currentReplicas + a
        if (currentReplicas.size > maxReplicas) maxReplicas = currentReplicas.size
        arbiterAudit.expectMsg(JoinedSecondary)
      }
      else None
    }

    def leave(replica: ActorRef) = {
      if (Random.nextInt(100) < 5) {
        replica ! Leave(replica)

        val a = arbiterAudit.expectMsgClass(classOf[Leave])
        currentReplicas = currentReplicas - a.replica

        println(s"LEAVING $replica")
      }
    }

    val ids = Iterator.from(0)

    def setOrRemove(key: Int) = {
      val client = session(primary)

      val id = ids.next()
      if (Random.nextInt(100) < 50) {
        client.set(s"k$key", (key*10).toString, id)
        client.waitAck(id)
        client.get(s"k$key").getOrElse(throw new Exception(s"bad insert primary k$key"))
        currentReplicas.foreach { replica =>
          val s = session(primary)
          s.get(s"k$key").getOrElse(throw new Exception(s"bad insert $replica k$key"))
        }
      }
      else {
        client.remove(s"k$key", id)
        client.waitAck(id)
        client.get(s"k$key").foreach(_ => throw new Exception(s"bad insert primary k$key"))
        currentReplicas.foreach { replica =>
          val s = session(primary)
          s.get(s"k$key").foreach(_ => throw new Exception(s"bad insert $replica k$key"))
        }
      }
    }

    // test with one initial replica
    // repeat the same keys concurrently
    join(true)
    join(true)
    val keys = Stream.continually(0 to 20).flatten.take(30)
    val futures = keys.map { i =>
      Future {
        setOrRemove(i)
        join(false)
        currentReplicas.foreach(leave)
      }
    }

    Await.result(Future.sequence(futures), Duration(30, TimeUnit.SECONDS))
    println(s"MAXIMUM OF REPLICAS ACTIVE: $maxReplicas")
  }
}
