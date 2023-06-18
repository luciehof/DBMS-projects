package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getLeftKeys]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getRightKeys]]
  */
class Join(
            left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
            right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
            condition: RexNode
          ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var equiTuples = List[Tuple]()

  private var groupLeft = Map[Tuple,List[Tuple]]()
  private var groupRight = Map[Tuple,List[Tuple]]()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()
    var leftTuplesBuf = ListBuffer[Tuple]()
    var rightTuplesBuf = ListBuffer[Tuple]()
    var equiTuplesBuf = ListBuffer[Tuple]()

    var nextLeft = left.next()
    var nextRight = right.next()
    while (nextLeft != NilTuple || nextRight != NilTuple) {
      if (nextLeft!= NilTuple) {
        leftTuplesBuf += nextLeft.get
        nextLeft = left.next()
      }
      if (nextRight != NilTuple) {
        rightTuplesBuf += nextRight.get
        nextRight = right.next()
      }
    }

    val leftTuples = leftTuplesBuf.toList
    val rightTuples = rightTuplesBuf.toList
    groupLeft = leftTuples.groupBy(getLeftKeys.map(_))
    groupRight = rightTuples.groupBy(getRightKeys.map(_))

    for ((key, listTuples) <- groupLeft) {
      if (groupRight.contains(key)) {
        listTuples.foreach(tLeft => {
          groupRight(key).foreach(tRight => {
            equiTuplesBuf += tLeft ++ tRight
          })
        })
      }
    }

    equiTuples = equiTuplesBuf.toList

    left.close()
    right.open()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (equiTuples.isEmpty) NilTuple
    else {
      val head = equiTuples.head
      equiTuples = equiTuples.tail
      Option(head)
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {}
}
