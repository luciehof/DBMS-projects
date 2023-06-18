package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{
  Elem,
  NilRLEentry,
  NilTuple,
  RLEentry,
  Tuple
}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Decode]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Decode protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Decode[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var leftInEntry: Long = -1

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    leftInEntry = 0
  }

  private var value = Option(IndexedSeq[Elem]())

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (leftInEntry <= 0) {
      val nextEntry = input.next()
      if (nextEntry == NilTuple) value = NilTuple
      else {
        leftInEntry = nextEntry.get.length
        value = Option(nextEntry.get.value)
      }
    }
    leftInEntry -= 1
    value
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
    leftInEntry = -1
  }
}
