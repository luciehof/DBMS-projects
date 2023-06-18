package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.collection.mutable.ListBuffer

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]

  private def getColumns: IndexedSeq[HomogeneousColumn] = {
    var colBuffer = ListBuffer[HomogeneousColumn]()
    for (i <- 0 until getRowType.getFieldCount) {
      colBuffer += scannable.getColumn(i)
    }
    colBuffer.toIndexedSeq
  }

  /**
    * @inheritdoc
    */
  def execute(): IndexedSeq[HomogeneousColumn] = {
    val columns = getColumns
    val trueCol = for (_ <- 0L until scannable.getRowCount) yield true
    columns :+ trueCol
  }
}
