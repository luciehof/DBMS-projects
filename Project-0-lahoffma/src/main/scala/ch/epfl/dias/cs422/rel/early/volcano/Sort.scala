package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.collection.convert.ImplicitConversions.{`buffer AsJavaList`, `list asScalaBuffer`}
import scala.math.Ordered.orderingToOrdered
import scala.util.control.Breaks.{break, breakable}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var sorted = List[Tuple]()
  private val fieldCollations = collation.getFieldCollations

  private def orderFrom(collation:  RelFieldCollation): (Tuple, Tuple) => Boolean = {
    (x,y) => {
      val fieldIdx = collation.getFieldIndex
      val direction = collation.getDirection
      if (direction.isDescending) {
        x(fieldIdx).asInstanceOf[Comparable[Elem]] > y(fieldIdx).asInstanceOf[Comparable[Elem]]
      } else {
        x(fieldIdx).asInstanceOf[Comparable[Elem]] < y(fieldIdx).asInstanceOf[Comparable[Elem]]
      }
    }
  }
  // for each collation, sort input on it
  // remove unecessary inputs in between collations sort
  private def slicedSort(): Unit = {

  }

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    sorted = input.toList
    fieldCollations.reverse.forEach(fieldCollation => sorted = sorted.sortWith(orderFrom(fieldCollation)))
    input.close()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (sorted.isEmpty) NilTuple
    else {
      val nextTuple = sorted.head
      sorted = sorted.tail
      Option(nextTuple)
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    sorted = List[Tuple]()
  }
}
