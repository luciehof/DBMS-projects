package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val leftTuples = left.execute().transpose.filter(_.last.asInstanceOf[Boolean])
    val rightTuples = right.execute().transpose.filter(_.last.asInstanceOf[Boolean])
    if (leftTuples.isEmpty || rightTuples.isEmpty) { IndexedSeq.empty } else {
      var joined = ListBuffer[Column]()
      val leftHash = leftTuples.map(getLeftKeys.map(_).hashCode()).zipWithIndex
      val rightHash = rightTuples.map(getRightKeys.map(_).hashCode()).zipWithIndex
      leftHash.foreach(h1 =>
          rightHash.foreach(h2 =>
            if (h1._1.equals(h2._1)) {
              joined += leftTuples(h1._2).dropRight(1) ++ rightTuples(h2._2)
            }
          )
      )
      joined.toIndexedSeq.transpose.map(toHomogeneousColumn)
    }
  }
}
