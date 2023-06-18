package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilRLEentry, NilTuple, RLEentry}

import scala.annotation.tailrec

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Reconstruct]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Reconstruct protected (
                              left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
                              right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
                            ) extends skeleton.Reconstruct[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
](left, right)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  private var nextLeft: Option[RLEentry] = NilRLEentry
  private var nextRight: Option[RLEentry] = NilRLEentry

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()
    nextLeft = left.next()
    nextRight = right.next()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    recursiveNext
  }

  @tailrec
  private def recursiveNext: Option[RLEentry] = {
    if (nextLeft == NilRLEentry || nextRight == NilRLEentry) {
      NilRLEentry
    } else {
      // get indexes intersection from nextLeft and nextRight,
      // if last indexes are different, keep longest one as nextRespectively
      // else put both to next
      val leftEntry = nextLeft.get
      val rightEntry = nextRight.get
      val endVID = Math.min(rightEntry.endVID, leftEntry.endVID)
      val startVID = Math.max(rightEntry.startVID, leftEntry.startVID)
      val length = endVID + 1 - startVID
      if (rightEntry.startVID>leftEntry.endVID) {
        nextLeft = left.next()
        recursiveNext
      } else if(leftEntry.startVID>rightEntry.endVID) {
        nextRight = right.next()
        recursiveNext
      } else {
        val value = leftEntry.value ++ rightEntry.value
        if (rightEntry.endVID == endVID) {
          nextRight = right.next()
        }
        if (leftEntry.endVID == endVID) {
          nextLeft = left.next()
        }
        Option(RLEentry(startVID, length, value))
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
    nextLeft = NilRLEentry
    nextRight = NilRLEentry
  }
}
