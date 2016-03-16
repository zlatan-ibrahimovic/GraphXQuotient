package com.bordeaux.projet

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._
import Console._
import org.apache.spark.rdd.RDD

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
		var test = new BuilderGraph(graph,"Stanford web graph")
		var subtest = new BuilderGraph(test.makeSubGraphWith(List(1,2,3)), "Stanford with vertices (1,2,3)")
		var t = sc.parallelize(Array((1, 1), (2, 1), (3,1)))
		var t2 = sc.parallelize(Array((1L, 1), (2L, 1), (3L,1)))
	
		var subtest2 = new BuilderGraph(test.makeSubGraphWithoutRDD(t2), "Stanford without RDD(1,2,3)")
		var subtest3 = new BuilderGraph(test.makeSubGraphWithRDD(t), "Stanford with RDD[Int,Int]={(1,1),(2,1),(3,1)}")
		

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
		
		var subtest3_id = subtest3.id
		var subtest3_nbrVertices = subtest3.nbrVertices
		var subtest3_edges_count = subtest3.edges.count
		//test graphQ 
		//var P = List((subtest.id,subtest.graphX), 
		//	(subtest2.id,subtest2.graphX))

		color(Console.BOLD,  Console.MAGENTA) {
		  	println(" ...................................")
			println(" :       TEST BuilderGraph         :")
			println(" :.................................:\n")

			println("> GRAPHX: Number of vertices " + graph_vertices_count)
			println("> GRAPHX: Number of edges " + graph_edges_count)
			
				
		
			print("> ")
			print(test_id)
			println(" Number of vertices " + test_nbrVertices)
			print("> ")
			print(test_id)
			println(" Number of edges " + test_edges_count)
			
			
			print("> ")
			print(subtest_id)
			println(" Number of vertices " + subtest_nbrVertices)
			print("> ")
			print(subtest_id)
			println(" Number of edges " + subtest_edges_count)
			
			
			
			
			print("> ")
			print(subtest2_id)
			println(" Number of vertices " + subtest2_nbrVertices)
			print(">")
			print(subtest2_id)
			println(" Number of edges " + subtest2_edges_count)
			

			print("> ")
			print(subtest3_id)
			println(" Number of vertices " + subtest3_nbrVertices)
			print("> ")
			print(subtest3_id)
			println(" Number of edges " + subtest3_edges_count)
			
			println("======= > Vertices of " + subtest2_id)
			//subtest.printVertices()
			println(subtest2.vertices)

			println(".......................................................")
		}
			
		var part1RDD = sc.parallelize(Array((1, 1), (2, 1), (3,1)))
		var part2RDD = sc.parallelize(Array((6548,1)))
		var partition1 = new BuilderGraph(test.makeSubGraphWithRDD(part1RDD), "partition1 with RDD 1,2,3")
		var partition2 = new BuilderGraph(test.makeSubGraphWithRDD(part2RDD), "partition2 with RDD 6548")
	
		//partition 1
		var partition1_id = partition1.id
		var partition1_nbrVertices = partition1.nbrVertices
		var partition1_edges_count = partition1.edges.count
	
		//partition 2
		var partition2_id = partition2.id
		var partition2_nbrVertices = partition2.nbrVertices
		var partition2_edges_count = partition2.edges.count
		
		//graphQuotient
		var graphQuotient = new GraphQ(sc,graph,List(("part1",partition1.graphX),("part2",partition2.graphX)), "graphQuotient")

		var graphQuotient_QG_nbrVertices = graphQuotient.QG.vertices.count
		var graphQuotient_QG_nbrEdges  = graphQuotient.QG.edges.count 


		color(Console.BOLD,  Console.MAGENTA) {
		  	println(" ...................................")
			println(" :       TEST GraphQuotient        :")
			println(" :.................................:\n")


			print("> ")
			print(partition1_id)
			println(" Number of vertices " + partition1_nbrVertices)
			print("> ")
			print(partition1_id)
			println(" Number of edges " + partition1_edges_count)
			

			print("> ")
			print(partition2_id)
			println(" Number of vertices " + partition2_nbrVertices)
			print("> ")
			print(partition2_id)
			println(" Number of edges " + partition2_edges_count)

			print("> ")
			print(graphQuotient.id)
			println(" Number of vertices " + graphQuotient_QG_nbrVertices)
			print("> ")
			print(graphQuotient.id)
			println(" Number of edges " + graphQuotient_QG_nbrEdges)

			graphQuotient.QG.vertices.collect.foreach(
        	println(_))
			println(".......................................................")
	

		}
	}

}