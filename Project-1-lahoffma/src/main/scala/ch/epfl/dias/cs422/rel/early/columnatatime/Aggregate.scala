package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  //.transpose.map(toHomogeneousColumn)

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val cols = input.execute()
    if (cols.dropRight(1).isEmpty && groupSet.isEmpty) {
      IndexedSeq(aggCalls.map(aggEmptyValue) :+ true).transpose
        .map(toHomogeneousColumn)
    } else {
      val grouped = cols.transpose
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
          .map(toHomogeneousColumn)
      }
    }
  }
}
