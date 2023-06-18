package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Aggregate protected (
                            input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
                            groupSet: ImmutableBitSet,
                            aggCalls: IndexedSeq[AggregateCall]
                          ) extends skeleton.Aggregate[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](input, groupSet, aggCalls)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var aggEmptyVal = false
  private var grouped = List[(Tuple, Iterable[Tuple])]()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    val inVector = input.toIndexedSeq
    aggEmptyVal = inVector.isEmpty && groupSet.isEmpty
    if (!aggEmptyVal) {
      grouped = inVector.groupBy(t => t.zipWithIndex.filter(e => groupSet.get(e._2)).map(_._1)).toList
    }
    input.close()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (aggEmptyVal) {
      aggEmptyVal = false
      Option(aggCalls.map(aggEmptyValue))
    } else if (grouped.isEmpty){
      NilTuple
    } else {
      val group = grouped.head
      grouped = grouped.tail
      Option(group._1 ++ aggCalls.map(agg => group._2.map(agg.getArgument(_)).reduce(agg.reduce(_,_))))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {}
}
