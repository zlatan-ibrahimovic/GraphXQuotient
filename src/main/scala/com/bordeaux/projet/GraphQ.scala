package com.bordeaux.projet

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import Array._
import scala.collection.mutable.WrappedArray
/*
C'est quoi un graphe quotient ?
  - on dispose d'un Graphe G et d'un partitionnement P
  -  => chaque nœud n de G appartient donc à une partition p de P
  - le graphe quotient QG est un nouveau graphe qui :
    - a autant de nœud que le nombre de partitions de P :  |P|
    - chaque nœud q de QG représente donc l'ensemble des nœuds d'une partition p de P et le sous graph induit
    - Il y a une arrête entre qi et qj si il y a un nœud de pi qui a une arrête avec un nœud de pj
*/

class GraphQ(var sc: SparkContext, var G: Graph[Int, Int], var P: List[(String,Graph[Int,Int])], var idQ: String) {
	var partition : List[(String,Graph[Int,Int])] = P //stocker dans une RDD
	var graph: Graph[Int,Int] = G
	
	var QG: Graph[(String,String),String] = this.buildGraphQ()
	var id = idQ  

	def nbrNodes(): Int ={
		return partition.size.toInt
	}

	def buildGraphQ():Graph[(String,String),String] ={
		//create RDD for the vertices
		var tmp : (VertexId, (String, String)) =
			(1L,(partition(0)._1,idQ))
		var arrayVertices = Array(tmp) 
		for( i <- 1 to this.nbrNodes()-1 ) {
			tmp = ((i+1).toLong,(partition(i)._1,idQ))
			arrayVertices = concat (arrayVertices,Array(tmp))
		}
		val partionVerticesRDD : RDD[(VertexId, (String, String))] =
		 sc.parallelize(arrayVertices.toSeq)


		// Create an RDD for edges
		// if subgraph(part_i + part_j) has more 
		//  edges than subgraph(part_i) + subgraph(part_j)
		// there is a edge between qi € qj 
		var arrayEdges : Array[Edge[String]] = Array[Edge[String]]()
		for( i <- 0 to this.nbrNodes()-2 ) {
			for( j <- i+1 to this.nbrNodes()-1) {
				 var newVertices = partition(i)._2.vertices ++ partition(j)._2.vertices
   				 var tmp = newVertices.collect
   				 var testEdge = graph.subgraph(vpred = (id, attr)
     			 => (tmp.exists { case (elem,at) => id == elem.toInt}))
				
				if(testEdge.edges.count > 
					(partition(i)._2.edges.count 
					+ partition(j)._2.edges.count)){
					arrayEdges = concat (arrayEdges, 
						Array(Edge((i+1).toLong,(j+1).toLong,"connect")))
				}
			}
		}
		val relationships: RDD[Edge[String]] =
  		 sc.parallelize(arrayEdges)

  		 //return de graphQ
		return Graph(partionVerticesRDD,relationships)

		//def getPartition()

	}



}