package com.bordeaux.projet

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._
import scala.collection.mutable.ListBuffer

class GraphQuotient(var graph: Graph[Int, Int], var idQ: String) {
  val graphX : Graph[Int,Int] = graph
  var vertices : List[Int] = this.verticesConvert()
  var edges = graph.edges
  var id : String = idQ
  var nbrVertices :Int =  graph.vertices.count.toInt
  
  def makeSubGraphWith(vertices: List[Int]): Graph[Int,Int]={
  	var res = graphX.subgraph(vpred = (id, attr)
  		=> (vertices.exists{ elem =>  id==elem} ))
	return res
  }


  def makeSubGraphWithout_v1(vertices: List[Int]): Graph[Int,Int]={
  	var res= graphX.subgraph(vpred = (id, attr) 
  		=> (vertices.exists{ elem =>  id!=elem} ))
	return res
  }


  def makeSubGraphWithout(vertices: List[Int]): Graph[Int,Int]={
    var newVertices = this.vertices diff vertices
    var res= graphX.subgraph(vpred = (id, attr) 
      => (newVertices.exists{ elem =>  id==elem} ))
  return res
  }
  def printVertices(){
    graphX.vertices.collect.foreach(
        println(_))
  }

  def printEdges(){
    graphX.edges.collect.foreach(
        println(_))
  }
  def verticesConvert() : List[Int]= {
    var tmp  = graphX.vertices.collect.toList
    var l : List[Int] = Nil
    for( (id,attr) <- tmp) {
      l = id.toInt :: l
    }
    return l
  }
}
