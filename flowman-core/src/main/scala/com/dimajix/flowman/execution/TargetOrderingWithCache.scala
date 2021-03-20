package com.dimajix.flowman.execution

import com.dimajix.flowman.model.{MappingOutputIdentifier, Target, TargetIdentifier}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class TargetOrderingWithCache {
    val mappingGraph = new MappingGraph()
    // each node in the DAG will know all targets depending on it => ( dependentTargets, treeDepth )
    private val targetsByMappings = mutable.Map[MappingOutputIdentifier, mutable.Set[TargetIdentifier]]()
    private val mappingsToCache = mutable.Set[MappingOutputIdentifier]()

    def traverseDependencies(
        node: Node,
        currentTarget: Target,
        targetDistance: Int = 1,
        branchVisited: Boolean = false
    ): Seq[( MappingOutputIdentifier, Int )] = {
        val branchIntersection = targetsByMappings.get(
            node.mappingOutputIdentifier
        ) match {
            case Some( previousTargets: mutable.Set[TargetIdentifier] ) => {
                if ( !branchVisited ) {
                    mappingsToCache += node.mappingOutputIdentifier
                }
                previousTargets += currentTarget.identifier
                true
            }
            case None => {
                // for this branch always holds true: currentTargets.size == 1
                targetsByMappings += ( node.mappingOutputIdentifier -> mutable.Set( currentTarget.identifier ) )
                false
            }
        }

        val dependencies = node
            .children
            .flatMap( child => {
                traverseDependencies(
                    child,
                    currentTarget,
                    targetDistance + 1,
                    branchIntersection
                )
            })
            .toSeq

        dependencies :+ ( ( node.mappingOutputIdentifier, targetDistance ) )
    }

    def getChildNodes( node: Node ): Seq[Node] = {
        node.children.toSeq
    }

    def getParentNodes( node: Node ): Seq[Node] = {
        node.parents.toSeq
    }

    def traverseForCachesSelfIncluded( traversalDirection: Node => Seq[Node], mappingOutputIdentifier: MappingOutputIdentifier ): Seq[Node] = {
        traverseForCaches(
            traversalDirection,
            mappingGraph.nodeMapping( mappingOutputIdentifier )
        )
    }

    def traverseForCachesSelfExcluded( traversalDirection: Node => Seq[Node], mappingOutputIdentifier: MappingOutputIdentifier ): Seq[Node] = {
        traversalDirection( mappingGraph.nodeMapping( mappingOutputIdentifier ) )
            .flatMap( traverseForCaches( traversalDirection, _ ))
    }

    def traverseForCaches( traversalDirection: Node => Seq[Node], node: Node ): Seq[Node] = {
        if( mappingsToCache.contains( node.mappingOutputIdentifier ) ){
            Seq( node )
        } else {
            traversalDirection( node )
                .flatMap( traverseForCaches( traversalDirection, _ ) )
        }
    }

    /**
     * Create ordering of specified targets, such that all dependencies are fulfilled
     * @param targets
     * @return
     */
    def sort(
        targets: Seq[Target],
        phase: Phase,
        fixedTargets: Seq[Target] = Seq(),
        fixedCaches: Seq[MappingOutputIdentifier] = Seq()
    ): Seq[Target] = {
        if ( targets.isEmpty ) {
            throw new IllegalArgumentException
        }

        //val logger = LoggerFactory.getLogger(classOf[Target])
        val context = targets.head.context
        val targetDependencies = targets
            .map( target => {
                val dependencies = mappingGraph
                    .getTargetsDependencyGraph( target, phase ) // after graph creation getting the entry node will be cheap
                    .flatMap( targetNode => {
                        traverseDependencies( targetNode, target ) // fill up "mappingsToCache"
                    })

                ( target.identifier, dependencies )
            })
            .toMap

        // sort targets by "least caches used first"
        val targetSortedByDownstreamCacheCount = targets
            .filterNot( target => fixedTargets.map( _.identifier ).contains( target.identifier ) ) // do not consider targets already set in stone
            .map{ target => {
                val downstreamCacheMappings = target
                    .mappings( phase )
                    .flatMap( targetMapping => {
                        val targetNode = mappingGraph.nodeMapping( targetMapping )
                        val downstreamCachesOrSources = traverseForCaches( getChildNodes, targetNode )
                        val downstreamCaches = downstreamCachesOrSources
                            .map( _.mappingOutputIdentifier )
                            .filter( mappingsToCache.contains )

                        downstreamCaches
                    })

                val allCachesForTarget = targetDependencies( target.identifier)
                    .filter{ case( mappingOutputIdentifier: MappingOutputIdentifier, depth: Int ) => {
                        mappingsToCache.contains( mappingOutputIdentifier )
                    }}

                val reusedCaches = targetDependencies( target.identifier)
                    .filter( mappingDependency => fixedCaches.contains( mappingDependency._1 ) )

                val reusableCaches = targetDependencies( target.identifier)
                    .flatMap( targetDependencyMappings => traverseForCachesSelfIncluded( getChildNodes, targetDependencyMappings._1 ) )
                    .filter( node => mappingsToCache.contains( node.mappingOutputIdentifier ) )
                    .flatMap( cacheNodes => {
                        traverseForCachesSelfExcluded( getParentNodes, cacheNodes.mappingOutputIdentifier )
                            .map( node => node.mappingOutputIdentifier )
                    })
                    .filterNot{ mapping => { // cache mappings only
                        ( target +: fixedTargets ).flatMap( _.mappings( phase ) ).contains( mapping )
                    }}

                val priorityMetric = (
                    -reusedCaches.size,
                    allCachesForTarget.size,
                    downstreamCacheMappings.size,
                    -reusableCaches.size
                )

                ( target.identifier, downstreamCacheMappings, reusedCaches, priorityMetric )
            }}
            .minBy{ _._4 }

        val currentTarget = context.getTarget( targetSortedByDownstreamCacheCount._1 )
        val newCacheMappings = targetDependencies( currentTarget.identifier )
            .map( _._1 )
            .filterNot( fixedCaches.contains )
        val newCacheTargets = newCacheMappings
            .map( cacheMapping => {
                createCacheTarget(
                    cacheMapping,
                    fixedTargets,
                    context
                )
            })

        // check for each target if any cache used so far can be freed
        val newTargets = ( newCacheTargets :+ currentTarget )
            .flatMap( target => {
                target +: target
                    .mappings( phase ) // get mappings used by target
                    .flatMap( mapping => {
                        // get downstream caches for target
                        if ( target.kind == "Cache" ) {
                            // if its a CacheTarget it will always depend on a cache mapping and exit immediately
                            traverseForCachesSelfExcluded( getChildNodes, mapping )
                        } else {
                            traverseForCachesSelfIncluded( getChildNodes, mapping )
                        }
                    })
                    .map( _.mappingOutputIdentifier ) // use MappingOutputIdentifier in favor of Node
                    .filter( fixedCaches.contains ) // only keep previously cached once
                    .flatMap( mapping => { // only keep "not used in the future anymore" (not used by currently still free Targets)
                        traverseForCachesSelfExcluded( getParentNodes, mapping ) // get targets using this mapping
                            .map( _.mappingOutputIdentifier )
                            .:+( mapping ) // traversal "self excluded" misses current mapping which will be needed later
                            .filterNot( potentialyFreedMapping => { // only keep mappings not used by free targets anymore
                                targets
                                    .filterNot( filterTarget => {  // get only free Targets
                                        ( fixedTargets :+ target ) // since it is about Mappings used by Targets add the current one
                                            .map( _.identifier )
                                            .contains( filterTarget.identifier )
                                    })
                                    //.++( newCacheTargets )
                                    .flatMap( target => { // get first order CacheMapping from those targets
                                        target
                                            .mappings( phase ) // get direct Mappings used by target
                                            .flatMap( targetMapping => { // collect next CacheMapping in front of Target
                                                traverseForCachesSelfIncluded( getChildNodes, targetMapping )
                                            })
                                            .map( _.mappingOutputIdentifier )
                                    })
                                    .contains( potentialyFreedMapping ) // check if is used later by another Target
                            })
                    })
                    .toSeq
                    .map( uncacheMapping => {
                        createUncacheTarget(
                            uncacheMapping,
                            target.identifier,
                            context
                        )
                    })
            })

        // only continue if there will be at least one target left next iteration
        if( fixedTargets.size < targets.size - 1 ) {
            val downstreamTargets = sort(
                targets,
                phase: Phase,
                fixedTargets :+ currentTarget,
                ( fixedCaches ++ newCacheMappings ).distinct
            )

            newTargets ++ downstreamTargets
        } else {
            newTargets
        }
    }

    def createCacheTarget(
        currentCacheMapping: MappingOutputIdentifier,
        priorTargets: Seq[Target],
        context: Context
    ): CacheTarget = {
        val targets = targetsByMappings( currentCacheMapping ).toSeq

        val cacheProperties = Target.Properties(
            context = context,
            namespace = context.namespace,
            project = context.project,
            before = targets,
            after = priorTargets.map( _.identifier ),
            labels = Map[String,String](),
            kind = "Cache",
            name = "cached_" + currentCacheMapping.name + "_" + java.util.UUID.randomUUID
        )

        CacheTarget(
            cacheProperties,
            currentCacheMapping
        )
    }

    def createUncacheTarget(
        currentCacheMapping: MappingOutputIdentifier,
        uncacheTarget: TargetIdentifier,
        context: Context
    ): UncacheTarget = {
        val uncacheProperties = Target.Properties(
            context = context,
            namespace = context.namespace,
            project = context.project,
            before = Seq(),
            after = Seq( uncacheTarget ),
            labels = Map[String,String](),
            kind = "Uncache",
            name = "uncached_" + currentCacheMapping.name + "_" + java.util.UUID.randomUUID
        )

        UncacheTarget(
            uncacheProperties,
            currentCacheMapping
        )
    }
}
