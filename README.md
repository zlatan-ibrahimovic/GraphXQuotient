# GraphXQuotient
Ped M2

Lancer Spark
./opt/spark-1.6.0-bin-hadoop2.4/bin/spark-submit 
--class com.bordeaux.projet.Test 
--master local 
target/projet-0.0.1-SNAPSHOT-jar-with-dependencies.jar data.txt local




J'ai un problème pour l'implémentation de la class Graphquotient 
pour créer un sous graph ayant des sommets qui ne sont pas dans une liste de noeud 
La première version ne le fait pas et la seconde prends du temps à compiler


def makeSubGraphWithout_v1(vertices: List[Int]): Graph[Int,Int]={
var res= graphX.subgraph(vpred = (id, attr) 
=> (vertices.exists{ elem => id!=elem} ))
return res
}


def makeSubGraphWithout(vertices: List[Int]): Graph[Int,Int]={
var newVertices = this.vertices diff vertices
var res= graphX.subgraph(vpred = (id, attr) ..
=> (newVertices.exists{ elem => id==elem} ))
return res
}
