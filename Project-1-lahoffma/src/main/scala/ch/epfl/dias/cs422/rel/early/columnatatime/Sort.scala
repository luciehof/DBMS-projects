package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.collection.convert.ImplicitConversions.{`buffer AsJavaList`, `list asScalaBuffer`}
import scala.jdk.CollectionConverters._
import scala.math.Ordered.orderingToOrdered

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  private var sorted = IndexedSeq[Tuple]()
  private val fieldCollations = collation.getFieldCollations
  private var OFFSET = -1
  private var FETCH = -1

  private def orderFrom(
      collation: RelFieldCollation
  ): (Tuple, Tuple) => Boolean = { (x, y) =>
    if (x.last.asInstanceOf[Boolean] && y.last.asInstanceOf[Boolean]) {
      val fieldIdx = collation.getFieldIndex
      val direction = collation.getDirection
      if (direction.isDescending) {
        x(fieldIdx).asInstanceOf[Comparable[Elem]] > y(fieldIdx)
          .asInstanceOf[Comparable[Elem]]
      } else {
        x(fieldIdx).asInstanceOf[Comparable[Elem]] < y(fieldIdx)
          .asInstanceOf[Comparable[Elem]]
      }
    } else
      true // if any of x or y is inactive, we don't care about order between the 2
  }

  private def slicedSort(list: List[Tuple], fieldCollation: RelFieldCollation): List[Tuple] = {
    val fieldIdx = fieldCollation.getFieldIndex
    var firstDrop = 0
    var lastDrop = 0
    var lastOffsetIdx = 0
    var lastFetchIdx = 0
    // verify OFFSET < list.size and FETCH too
    if (FETCH > list.size) FETCH = list.size
    if (OFFSET >= list.size) {
      OFFSET = 0
      FETCH = 0
      firstDrop = list.size
      lastOffsetIdx = list.size -1
      lastFetchIdx = list.size -1
    } else if (OFFSET>0 && FETCH>0) {
      lastOffsetIdx = OFFSET -1
      lastFetchIdx = OFFSET+FETCH-1
    } else if (OFFSET==0 && FETCH>0) {
      lastOffsetIdx = 0
      lastFetchIdx = FETCH-1
    } else if (OFFSET>0 && FETCH==0) {
      lastOffsetIdx = OFFSET -1
      lastFetchIdx = 0
    }
    val lastEltOffset = list(lastOffsetIdx)(fieldIdx).asInstanceOf[Comparable[Elem]]
    val lastEltFetch = list(lastFetchIdx)(fieldIdx).asInstanceOf[Comparable[Elem]]

    if (OFFSET!=0 && OFFSET < list.size && lastEltOffset != list(OFFSET)(fieldIdx).asInstanceOf[Comparable[Elem]]) {
      // if field at offset-1 != at offset: offset to be dropped
      firstDrop = OFFSET
    } else if (OFFSET!=0 && list.head(fieldIdx).asInstanceOf[Comparable[Elem]] != lastEltOffset) {
      // else if field at 0 != at offset-1: first N rows with diff field to be dropped and offset -= N
      var i = OFFSET-2
      var otherTupElem = list(i)(fieldIdx).asInstanceOf[Comparable[Elem]]
      while (otherTupElem == lastEltOffset && i>0) {
        i -= 1
        otherTupElem = list(i)(fieldIdx).asInstanceOf[Comparable[Elem]]
      } // out of while loop, elt at i is same as refTupElem, so we drop elts until i-1
      firstDrop = i
    }
    if (FETCH!=0 && OFFSET+FETCH < list.size && lastEltFetch != list(OFFSET+FETCH)(fieldIdx).asInstanceOf[Comparable[Elem]]) {
      // if field at offset+fetch-1 != at offset+fetch: elts after (included) offset+fetch to be dropped
      lastDrop = list.size - (OFFSET+FETCH)
    } else if (FETCH!=0 && lastEltFetch != list.last(fieldIdx).asInstanceOf[Comparable[Elem]]) {
      // else if field at last != at offset+fetch-1: last M rows with diff field to be dropped
      var j = OFFSET+FETCH
      var otherTupElem = list(j)(fieldIdx).asInstanceOf[Comparable[Elem]]
      while (otherTupElem == lastEltFetch && j<list.size) {
        j += 1
        otherTupElem = list(j)(fieldIdx).asInstanceOf[Comparable[Elem]]
      }
      lastDrop = list.size - j
    }
    OFFSET -= firstDrop
    list.drop(firstDrop).dropRight(lastDrop-firstDrop)
  }

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val cols = input.execute()
    if (cols.dropRight(1).isEmpty) { IndexedSeq.empty }
    else {
      OFFSET = offset.getOrElse(0)
      FETCH = fetch.getOrElse(0)
      sorted = cols.transpose
      fieldCollations.reverse.forEach(fieldCollation =>
        sorted = sorted.sortWith(orderFrom(fieldCollation))
      )
      sorted.transpose.map(toHomogeneousColumn)
    }
  }
}
