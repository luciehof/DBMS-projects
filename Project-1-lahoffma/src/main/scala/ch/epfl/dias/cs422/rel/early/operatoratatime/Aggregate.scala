package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, NilTuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[Column] = {
    val inVector = input.toIndexedSeq
    if (inVector.dropRight(1).isEmpty && groupSet.isEmpty) {
      IndexedSeq(aggCalls.map(aggEmptyValue) :+ true).transpose
    } else {
      val grouped = inVector.transpose
        .filter(_.last.asInstanceOf[Boolean])
        .groupBy(t => t.zipWithIndex.filter(e => groupSet.get(e._2)).map(_._1))
        .toIndexedSeq
      if (grouped.isEmpty) {
        IndexedSeq.empty :+ IndexedSeq(true)
      } else {
        grouped
          .map(group => {
            val key = group._1
            val tups = group._2
            key ++ aggCalls.map(agg =>
              tups
                .map(t => agg.getArgument(t.dropRight(1)))
                .reduce(agg.reduce(_, _))
            ) :+ true
          })
          .transpose
      }
    }
  }
}
