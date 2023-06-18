package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, RLEentry, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEAggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  private var aggEmptyVal = false
  private var grouped = Vector[(Tuple, Iterable[RLEentry])]()
  private var VID = -1

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    val inVector = input.toIndexedSeq
    aggEmptyVal = inVector.isEmpty && groupSet.isEmpty
    if (!aggEmptyVal) {
      grouped = inVector.groupBy(entry => entry.value.zipWithIndex.filter(e => groupSet.get(e._2)).map(_._1)).toVector
    }
    VID = 0
    input.close()
  }


  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (aggEmptyVal) {
      aggEmptyVal = false
      Option(RLEentry(VID, 1, aggCalls.map(aggEmptyValue)))
    } else if (grouped.isEmpty){
      NilTuple
    } else {
      val group = grouped.head
      grouped = grouped.tail
      val entries = group._2
      val value = group._1 ++ aggCalls.map(agg => entries.map(e => agg.getArgument(e.value, e.length)).reduce(agg.reduce(_,_)))
      val startVID = VID
      VID += 1
      Option(RLEentry(startVID, 1, value))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    VID = -1
    aggEmptyVal = false
    grouped = Vector[(Tuple, Iterable[RLEentry])]()
  }
}
