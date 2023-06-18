package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[Column] = {
    val leftVector = left.toIndexedSeq.transpose.filter(_.last.asInstanceOf[Boolean])
    val rightVector = right.toIndexedSeq.transpose.filter(_.last.asInstanceOf[Boolean])
    if (leftVector.isEmpty || rightVector.isEmpty) { IndexedSeq.empty } else {
      val leftHash = leftVector.map(getLeftKeys.map(_).hashCode()).zipWithIndex
      val rightHash = rightVector.map(getRightKeys.map(_).hashCode()).zipWithIndex
      var joined = ListBuffer[Column]()
      leftHash.foreach(h1 =>
          rightHash.foreach(h2 =>
            if (h1._1.equals(h2._1)) {
              joined += leftVector(h1._2).dropRight(1) ++ rightVector(h2._2)
            }
          )
      )
      joined.toIndexedSeq.transpose
    }
  }
}
