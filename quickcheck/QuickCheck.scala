package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  // inserting an element into an empty heap then findMin to yield element
  property("min1") = forAll { a: A =>
    val h = insert(a, empty)
    findMin(h) == a
  }
  
  // inserting two elements into an empty heap then findMin to yield smaller element (hint 1)
  property("min2") = forAll { (a: A, b: A) => 
    val h = insert(a, insert(b, empty))
    findMin(h) == min(a,b)
  }
  
  // inserting an element into an empty heap then deleting min should result in empty heap (hint 2)
  property("del1") = forAll { a: A => 
    isEmpty(deleteMin(insert(a,empty)))
  }
  
  // finding and deleting min elements from heap should result in an increasing sequence (hint 3)
  property("sortedDelete") = forAll { h: H =>
    def isSorted(h: H): Boolean = {
      if (isEmpty(h)) true
      else {
        val a = findMin(h)
        val h2 = deleteMin(h)
        if (!isEmpty(h2)) a <= findMin(h2) && isSorted(h2) else true
      }
    }
    isSorted(h)  
  }
  
  // find min of meld of two heaps should be same as min for one of them (hint 4)
  property("meldmin") = forAll { (h1: H, h2: H) =>
    findMin(meld(h1, h2)) == min(findMin(h1), findMin(h2))
  }
  
  // melding two heaps should yield same as moving min from one heap to another and melding the resulting two
  property("meldMinMove") = forAll { (h1: H, h2: H) =>
    def compareHeaps(h1: H, h2: H): Boolean = {
      if (!isEmpty(h1) && !isEmpty(h2)) {  
        findMin(h1) == findMin(h2) && compareHeaps(deleteMin(h1), deleteMin(h2))
      }
      else true
    }
    
    compareHeaps(meld(h1, h2), 
        meld(deleteMin(h1), insert(findMin(h1), h2)))
  }
  
  //supplementary method to find min of two values
  def min(a: A, b: A): A = {
    if (a < b)  a else  b
  }  

  lazy val genHeap: Gen[H] = for {
    a <- arbitrary[A]
    h <- oneOf(const(empty), genHeap)
  } yield insert(a,h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
