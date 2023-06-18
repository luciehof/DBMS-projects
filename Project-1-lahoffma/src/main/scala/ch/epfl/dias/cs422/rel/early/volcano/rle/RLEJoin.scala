package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilRLEentry, RLEentry}
import org.apache.calcite.rex.RexNode

import scala.annotation.tailrec

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEJoin(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  private var nextLeft: Option[RLEentry] = NilRLEentry
  private var rightEntries = Vector[RLEentry]()
  private var rightIdx = -1
  private var VID = -1L

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()
    rightEntries = right.toVector
    right.close()
    nextLeft = left.next()
    rightIdx = 0
    VID = 0
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    recursiveNext
  }

  @tailrec
  private def recursiveNext: Option[RLEentry] = {
    if (nextLeft == NilRLEentry) NilRLEentry
    else if (rightIdx == rightEntries.size) {
      rightIdx = 0
      nextLeft = left.next()
      recursiveNext
    } else {
      val leftEntry = nextLeft.get
      val rightEntry = rightEntries(rightIdx)
      rightIdx += 1
      val tupLeft = leftEntry.value
      val tupRight = rightEntry.value
      if (getLeftKeys.map(tupLeft(_)).equals(getRightKeys.map(tupRight(_)))) {
        val startVID = VID
        val length = leftEntry.length * rightEntry.length
        VID += length
        Option(RLEentry(startVID, length, tupLeft ++ tupRight))
      } else {
        recursiveNext
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    rightEntries = Vector[RLEentry]()
    nextLeft = NilRLEentry
    rightIdx = -1
    VID = -1
  }
}
