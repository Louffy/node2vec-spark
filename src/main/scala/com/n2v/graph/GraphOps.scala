package com.n2v.graph

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeTriplet, Graph, Edge, _}
import org.apache.spark.rdd.RDD
import com.n2v.Main

object GraphOps {
  var context: SparkContext = _
  var config: Main.Params = _
  
  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.config = param
    
    this
  }

  /**
    * Alias Method : Sampling from a Discrete Distribution
    * http://www.keithschwarz.com/darts-dice-coins/
    * @param nodeWeights
    * @return
    */
  def setupAlias(nodeWeights: Array[(Long, Double)]): (Array[Int], Array[Double]) = {
    val K = nodeWeights.length
    val J = Array.fill(K)(0)
    val q = Array.fill(K)(0.0)

    val smaller = new ArrayBuffer[Int]()
    val larger = new ArrayBuffer[Int]()

    val sum = nodeWeights.map(_._2).sum
    nodeWeights.zipWithIndex.foreach { case ((nodeId, weight), i) =>
      q(i) = K * weight / sum
      if (q(i) < 1.0) {
        smaller.append(i)
      } else {
        larger.append(i)
      }
    }

    while (smaller.nonEmpty && larger.nonEmpty) {
      val small = smaller.remove(smaller.length - 1)
      val large = larger.remove(larger.length - 1)

      J(small) = large
      q(large) = q(large) + q(small) - 1.0
      if (q(large) < 1.0) smaller.append(large)
      else larger.append(large)
    }

    (J, q)
  }



  def drawAlias(J: Array[Int], q: Array[Double]): Int = {
    val K = J.length
    val kk = math.floor(math.random * K).toInt

    if (math.random < q(kk)) kk
    else J(kk)
  }

  def setupEdgeAlias(p: Double = 1.0,
                     q: Double = 1.0)(srcId: Long,
                                      srcNeighbors: Array[(Long, Double)],
                                      dstNeighbors: Array[(Long, Double)]): (Array[Int], Array[Double]) = {
    val neighbors_ = dstNeighbors.map { case (dstNeighborId, weight) =>
      var unnormProb = weight / q
      if (srcId == dstNeighborId) unnormProb = weight / p
      else if (srcNeighbors.exists(_._1 == dstNeighborId)) unnormProb = weight

      (dstNeighborId, unnormProb)
    }

    setupAlias(neighbors_)
  }
  
  def initTransitionProb(indexedNodes: RDD[(VertexId, NodeAttr)], indexedEdges: RDD[Edge[EdgeAttr]]) = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    
    val graph = Graph(indexedNodes, indexedEdges)
      .mapVertices[NodeAttr] { case (vertexId, nodeAttr) =>
      val (j, q) = GraphOps.setupAlias(nodeAttr.neighbors)
      val nextNodeIndex = GraphOps.drawAlias(j, q)
      nodeAttr.path = Array(vertexId, nodeAttr.neighbors(nextNodeIndex)._1)
      nodeAttr
    }.mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
      val (j, q) = GraphOps.setupEdgeAlias(bcP.value, bcQ.value)(edgeTriplet.srcId, 
        edgeTriplet.srcAttr.neighbors,
        edgeTriplet.dstAttr.neighbors)
      
      edgeTriplet.attr.J = j
      edgeTriplet.attr.q = q
      edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors.map(_._1)
      
      edgeTriplet.attr
    }.cache
    
    graph
  }
  
}
