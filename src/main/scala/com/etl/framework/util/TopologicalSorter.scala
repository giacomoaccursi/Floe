package com.etl.framework.util

import com.etl.framework.exceptions.CircularDependencyException

import scala.collection.mutable

object TopologicalSorter {

  case class SortedGroups(groups: Seq[Seq[String]])

  def buildGraph[T](
      items: Seq[T],
      idOf: T => String,
      dependenciesOf: T => Set[String]
  ): Map[String, Set[String]] = {
    val graph = mutable.Map[String, mutable.Set[String]]()
    items.foreach(item => graph.getOrElseUpdate(idOf(item), mutable.Set.empty))
    items.foreach { item =>
      dependenciesOf(item).foreach { dep =>
        graph.getOrElseUpdate(idOf(item), mutable.Set.empty).add(dep)
      }
    }
    graph.map { case (k, v) => k -> v.toSet }.toMap
  }

  def sort(
      graph: Map[String, Set[String]],
      graphType: String = "dependency graph"
  ): Seq[String] = {
    val sorted = mutable.ArrayBuffer[String]()
    val visited = mutable.Set[String]()
    val visiting = mutable.Set[String]()
    val path = mutable.ArrayBuffer[String]()

    def visit(node: String): Unit = {
      if (visiting.contains(node)) {
        val cycleStart = path.indexOf(node)
        val cycle = path.slice(cycleStart, path.length).toSeq :+ node
        throw CircularDependencyException(graphType = graphType, cycle = cycle)
      }
      if (!visited.contains(node)) {
        visiting.add(node)
        path.append(node)
        graph.getOrElse(node, Set.empty).foreach(visit)
        path.remove(path.length - 1)
        visiting.remove(node)
        visited.add(node)
        sorted.append(node)
      }
    }

    graph.keys.foreach(visit)
    sorted.toSeq
  }

  def group(
      sortedIds: Seq[String],
      graph: Map[String, Set[String]],
      parallelEnabled: Boolean
  ): Seq[(Seq[String], Boolean)] = {
    val groups = mutable.ArrayBuffer[(Seq[String], Boolean)]()
    val completed = mutable.Set[String]()
    val remaining = mutable.Queue(sortedIds: _*)

    while (remaining.nonEmpty) {
      val currentGroup = mutable.ArrayBuffer[String]()
      val nextRemaining = mutable.Queue[String]()

      while (remaining.nonEmpty) {
        val id = remaining.dequeue()
        if (graph.getOrElse(id, Set.empty).subsetOf(completed))
          currentGroup.append(id)
        else
          nextRemaining.enqueue(id)
      }

      if (currentGroup.nonEmpty) {
        groups.append((currentGroup.toSeq, parallelEnabled && currentGroup.size > 1))
        currentGroup.foreach(completed.add)
      }

      remaining.clear()
      remaining ++= nextRemaining
    }

    groups.toSeq
  }
}
