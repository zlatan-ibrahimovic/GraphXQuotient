package com.bordeaux.projet

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._
import Console._

object Test{
	def color[T](cols: String*)(f: => T) {
		cols.foreach(print)
		val r = f
		print(Console.RESET)
		r
	}
	def main(args: Array[String]) = {
		if (args.length < 2) {
			System.err.println(
					"Usage: <file> <setmaster>")
					System.err.println("  enter the graph's file path")
					System.err.println("  enter parameter setmaster")

					System.exit(1)
		}
		val fname = args(0)
				val conf = new SparkConf()
		.setAppName("Test")
		.setMaster(args(1))


		val sc = new SparkContext(conf)
		
		//Laod Graph
		val graph = GraphLoader.edgeListFile(sc, fname,false,-1,StorageLevel.MEMORY_ONLY,StorageLevel.MEMORY_ONLY)

		//number of _ GraphX
		var graph_vertices_count =	 graph.vertices.count
		var graph_edges_count = graph.edges.count

		//GraphQuotient
		var test = new GraphQuotient(graph,"Stanford web graph")
		var subtest = new GraphQuotient(test.makeSubGraphWith(List(1,2,3)), "Stanford with vertices (1,2,3)")
		var subtest2 = new GraphQuotient(test.makeSubGraphWithout(List(1,2,3)), "Stanford without vertices (1,2,3)")
		
		//printValues of GraphQuotient
		var test_id = test.id
		var test_nbrVertices = test.nbrVertices
		var test_edges_count = test.edges.count
		var subtest_id = subtest.id
		var subtest_nbrVertices = subtest.nbrVertices
		var subtest_edges_count = subtest.edges.count
		var subtest2_id = subtest2.id
		var subtest2_nbrVertices = subtest2.nbrVertices
		var subtest2_edges_count = subtest2.edges.count

		color(Console.BOLD,  Console.MAGENTA) {
		  	println(" ..................................")
			println(" :       TEST GRAPHQUOTIENT        :")
			println(" :.................................:\n")

			println("> GRAPHX: Number of vertices " + graph_vertices_count)
			println("> GRAPHX: Number of edges " + graph_edges_count)
			
				
		
			print(">")
			print(test_id)
			println(" Number of vertices " + test_nbrVertices)
			print("> ")
			print(test_id)
			println(" Number of edges " + test_edges_count)
			
			
			print(">")
			print(subtest_id)
			println(" Number of vertices " + subtest_nbrVertices)
			print(">")
			print(subtest_id)
			println(" Number of edges " + subtest_edges_count)
			
			
			print(">")
			print(subtest2_id)
			println(" Number of vertices " + subtest2_nbrVertices)
			print(">")
			print(subtest2_id)
			println(" Number of edges " + subtest2_edges_count)
			
			println("======= > Vertices of " + subtest_id)
			//subtest.printVertices()
			println(subtest.vertices)

			println(".............................................")
		}
	
	}

}