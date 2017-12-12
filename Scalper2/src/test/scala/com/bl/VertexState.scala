//package com.bl
//
//import org.apache.spark.graphx.{Graph, TripletFields}
//
//import scala.reflect.ClassTag
//
//class VertexState  extends Serializable {
//  var community = -1L
//  var communitySigmaTot = 0L
//  var internalWeight = 0L
//  var nodeWeight = 0L;
//  var changed = false
//  override def toString(): String = {
//    "{community:"+community+",communitySigmaTot:"+communitySigmaTot+
//      ",internalWeight:"+internalWeight+",nodeWeight:"+nodeWeight+"}"
//  }
//
//
//  def createLouvainGraph[VD:ClassTag ]( graph:Graph[VD,Long ] ):Graph[VertexState,Long]={
//    val nodeWeights = graph.aggregateMessages( nodeWeightMapFunc,nodeWeightReduceFunc,TripletFields.All )
//    val louvainGraph = graph.outerJoinVertices( nodeWeights )((id,vd,weightOption) =>{
//      val weight = weightOption.getOrElse(0L)
//      val state = new VertexState()
//      state.community = id
//      state.changed = false
//      state.communitySigmaTot = weight
//      state.internalWeight = 0L
//      state.nodeWeight = weight
//      state
//    }).groupEdges(_+_).partitionBy(PartitionStrategy.EdgePartition2D)
//    louvainGraph
//  }
//
//
//  def q( currCommunityId:Long,testCommunityId:Long,testSigmaTot:Long,edgeWeightInCommunity:Long,
//         nodeWeight:Long,internalWeight:Long,totalEdgeWeight:Long ):Double={
//    val isCurrentCommunity = currCommunityId == testCommunityId
//    val M = totalEdgeWeight
//    val k_i_in_L = if( isCurrentCommunity ) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity
//    val k_i_in =  k_i_in_L
//    val k_i =  nodeWeight + internalWeight
//    val sigma_tot = if (isCurrentCommunity) testSigmaTot - k_i else testSigmaTot
//    var deltaQ =  0.0
//    if (!(isCurrentCommunity && sigma_tot.equals(0.0))) {
//      deltaQ = k_i_in - ( k_i * sigma_tot / M)
//    }
//    d(-｡-;)ltaQ
//  }
//
//
//
//
//
//  val internalEdgeWeights = graph.triplets.flatMap(et=>{
//    if (et.srcAttr.community == et.dstAttr.community){
//      Iterator( ( et.srcAttr.community, 2*et.attr) )
//    }
//    else Iterator.empty
//  }).reduceByKey(_+_)
//  val internalWeights = graph.vertices.values.map(vdata=> (vdata.community,vdata.internalWeight)).reduceByKey(_+_)
//  val newVerts = internalWeights.leftOuterJoin(internalEdgeWeights).map({case (vid,(weight1,weight2Option)) =>
//    val weight2 = weight2Option.getOrElse(0L)
//    val state = new VertexState()
//    state.community = vid
//    state.changed = false
//    state.communitySigmaTot = 0L
//    state.internalWeight = weight1+weight2
//    state.nodeWeight = 0L
//    (vid,state)
//  }).cache()
//
//  val edges = graph.triplets.flatMap(et=> {
//    val src = math.min(et.srcAttr.community,et.dstAttr.community)
//    val dst = math.max(et.srcAttr.community,et.dstAttr.community)
//    if (src != dst) Iterator(new Edge(src, dst, et.attr))
//    else Iterator.empty
//  }).cache()
//
//  val compressedGraph = Graph(newVerts,edges)
//    .partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+_)
//  val nodeWeights = compressedGraph.aggregateMessages(nodeWeightMapFunc,nodeWeightReduceFunc)
//  val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vid,data,weightOption)=> {
//    val weight = weightOption.getOrElse(0L)
//    data.communitySigmaTot = weight +data.internalWeight
//    data.nodeWeight = weight
//    data
//  }).cache()
//}
