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
  var pos_x : Int =0
  var pos_y: Int =0
  var size: Int = 0
  var adjacents: List[GraphQuotient] = Nil
  var nodes: List[GraphQuotient] = Nil



 //function to create subgraph with graphX 
  def makeSubGraphWith(vertices: List[Int]): Graph[Int,Int]={
  	var res = graphX.subgraph(vpred = (id, attr)
  		=> (vertices.exists{ elem =>  id==elem} ))
	return res
  }


  def makeSubGraphWithout(vertices: List[Int]): Graph[Int,Int]={
  	var res= graphX.subgraph(vpred = (id, attr) 
  		=> (vertices.exists{ elem =>  !(id equals elem)} ))
	return res
  }


  def makeSubGraphWithout_v2(vertices: List[Int]): Graph[Int,Int]={
    var newVertices = this.vertices diff vertices
    var res= graphX.subgraph(vpred = (id, attr) 
      => (newVertices.exists{ elem =>  id==elem} ))
  return res
  }

  def makeSubGraphWithout_v3(vertices: List[Int]): Graph[Int,Int]={
    var res= graphX.subgraph(vpred = (id, attr) 
      => (vertices.exists{ elem =>  id != elem } ))
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



  //function for graphQ with graphQ's vertices
  def edge(graphQ1: GraphQuotient,graphQ2: GraphQuotient ){
    graphQ1.addAdjacent(graphQ2)
    graphQ2.addAdjacent(graphQ1)
    this.addNode(graphQ1)
    this.addNode(graphQ2)
  }

  def addNode(node : GraphQuotient){
    this.nodes = node :: node.nodes ::: this.nodes
    this.nodes = this.nodes.distinct
  }

  def addAdjacent(adjacent : GraphQuotient){
    this.adjacents = adjacent :: this.adjacents  
  }

  
}
