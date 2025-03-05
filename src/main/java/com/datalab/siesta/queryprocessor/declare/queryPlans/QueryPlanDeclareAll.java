package com.datalab.siesta.queryprocessor.declare.queryPlans;

import com.datalab.siesta.queryprocessor.declare.DeclareDBConnector;
import com.datalab.siesta.queryprocessor.declare.model.*;
import com.datalab.siesta.queryprocessor.declare.queryPlans.existence.QueryPlanExistences;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsAlternate;
import com.datalab.siesta.queryprocessor.declare.queryPlans.orderedRelations.QueryPlanOrderedRelationsChain;
import com.datalab.siesta.queryprocessor.declare.queryPlans.position.QueryPlanPositions;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseDeclareAll;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseExistence;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponseOrderedRelations;
import com.datalab.siesta.queryprocessor.declare.queryResponses.QueryResponsePosition;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryPositionWrapper;
import com.datalab.siesta.queryprocessor.declare.queryWrappers.QueryWrapperDeclare;
import com.datalab.siesta.queryprocessor.model.DBModel.Metadata;
import com.datalab.siesta.queryprocessor.model.Queries.QueryPlans.QueryPlan;
import com.datalab.siesta.queryprocessor.model.Queries.QueryResponses.QueryResponse;
import com.datalab.siesta.queryprocessor.model.Queries.Wrapper.QueryWrapper;

import com.datalab.siesta.queryprocessor.storage.model.EventTypeTracePositions;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import scala.Tuple2;

import java.util.*;

@Component
@RequestScope
public class QueryPlanDeclareAll implements QueryPlan {

	private QueryPlanOrderedRelations queryPlanOrderedRelations;
	private QueryPlanOrderedRelationsChain queryPlanOrderedRelationsChain;
	private QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate;
	private QueryPlanExistences queryPlanExistences;
	private QueryPlanPositions queryPlanPositions;
	private JavaSparkContext javaSparkContext;
	private DeclareDBConnector declareDBConnector;
	@Setter
	private Metadata metadata;

	@Autowired
	public QueryPlanDeclareAll(QueryPlanOrderedRelations queryPlanOrderedRelations,
			QueryPlanOrderedRelationsChain queryPlanOrderedRelationsChain,
			QueryPlanOrderedRelationsAlternate queryPlanOrderedRelationsAlternate,
			QueryPlanExistences queryPlanExistences, QueryPlanPositions queryPlanPositions,
			JavaSparkContext javaSparkContext, DeclareDBConnector declareDBConnector) {
	        this.queryPlanOrderedRelations = queryPlanOrderedRelations;
		this.queryPlanOrderedRelationsAlternate = queryPlanOrderedRelationsAlternate;
		this.queryPlanOrderedRelationsChain = queryPlanOrderedRelationsChain;
		this.queryPlanExistences = queryPlanExistences;
		this.queryPlanPositions = queryPlanPositions;
		this.javaSparkContext = javaSparkContext;
		this.declareDBConnector = declareDBConnector;
	}

	@Override
	public QueryResponse execute(QueryWrapper qw) {
		QueryWrapperDeclare qwd = (QueryWrapperDeclare) qw;
		double support = qwd.getSupport();

		// run positions
		this.queryPlanPositions.setMetadata(metadata);
		QueryPositionWrapper qpw = new QueryPositionWrapper(support);
		QueryResponsePosition queryResponsePosition = (QueryResponsePosition) this.queryPlanPositions.execute(qpw);
		// run existences
		this.queryPlanExistences.setMetadata(metadata);
		this.queryPlanExistences.initResponse();

		// if existence, absence or exactly in modes
		Dataset<UniqueTracesPerEventType> uEventType = declareDBConnector
				.querySingleTableDeclare(this.metadata.getLogname());
		uEventType.persist(StorageLevel.MEMORY_AND_DISK());
		Map<String, HashMap<Integer, Long>> groupTimes = this.queryPlanExistences.createMapForSingle(uEventType);
		Map<String, Long> singleUnique = this.queryPlanExistences.extractUniqueTracesSingle(groupTimes);

		Dataset<UniqueTracesPerEventPair> uPairs = declareDBConnector
				.queryIndexTableDeclare(this.metadata.getLogname());
		Dataset<EventPairToNumberOfTrace> joined = this.queryPlanExistences.joinUnionTraces(uPairs);
		QueryResponseExistence queryResponseExistence = this.queryPlanExistences.runAll(groupTimes, support, joined,
				metadata.getTraces(), singleUnique, uEventType);
		uEventType.unpersist();

		// create joined table for order relations
		// load data from query table
		Dataset<EventPairToTrace> indexRDD = declareDBConnector.queryIndexOriginalDeclare(this.metadata.getLogname())
				.filter(functions.col("eventA").notEqual(functions.col("eventB")));
//
		Dataset<EventTypeTracePositions> singleRDD = declareDBConnector
				.querySingleTableAllDeclare(this.metadata.getLogname());
		singleRDD.persist(StorageLevel.MEMORY_AND_DISK());
//
		this.queryPlanOrderedRelations.initQueryResponse();
		this.queryPlanOrderedRelations.setMetadata(metadata);
		// join records from single table with records from the index table
		Dataset<EventPairTraceOccurrences> joinedOrder = this.queryPlanOrderedRelations.joinTables(indexRDD, singleRDD);
		joinedOrder.persist(StorageLevel.MEMORY_AND_DISK());
		singleRDD.unpersist();
//		 extract simple ordered
		Dataset<Abstract2OrderConstraint> cSimple = this.queryPlanOrderedRelations.evaluateConstraint(joinedOrder);
		cSimple.persist(StorageLevel.MEMORY_AND_DISK());
		// extract additional information required for pruning
		Dataset<EventTypeOccurrences> uEventType2 = declareDBConnector
				.extractTotalOccurrencesPerEventType(this.metadata.getLogname());
		// extract no-succession constraints from event pairs that do not exist in the
		// database log
		this.queryPlanOrderedRelations.extendNotSuccession(uEventType2, this.metadata.getLogname(), cSimple);
		// filter based on support
		this.queryPlanOrderedRelations.filterBasedOnSupport(cSimple, uEventType2, support);
		// load extracted constraints to the response
		QueryResponseOrderedRelations qSimple = this.queryPlanOrderedRelations.getQueryResponseOrderedRelations();
		cSimple.unpersist();

		// similar process to extract alternate ordered
		this.queryPlanOrderedRelationsAlternate.initQueryResponse();
		this.queryPlanOrderedRelationsAlternate.setMetadata(metadata);
		Dataset<Abstract2OrderConstraint> cAlternate = this.queryPlanOrderedRelationsAlternate
				.evaluateConstraint(joinedOrder);
		this.queryPlanOrderedRelationsAlternate.filterBasedOnSupport(cAlternate, uEventType2, support);
		QueryResponseOrderedRelations qAlternate = this.queryPlanOrderedRelationsAlternate
				.getQueryResponseOrderedRelations();

		// similar process to extract chain ordered
		this.queryPlanOrderedRelationsChain.initQueryResponse();
		this.queryPlanOrderedRelationsChain.setMetadata(metadata);
		Dataset<Abstract2OrderConstraint> cChain = this.queryPlanOrderedRelationsChain.evaluateConstraint(joinedOrder);
		cChain.persist(StorageLevel.MEMORY_AND_DISK());
		this.queryPlanOrderedRelationsChain.extendNotSuccession(uEventType2, this.metadata.getLogname(), cChain);
		this.queryPlanOrderedRelationsChain.filterBasedOnSupport(cChain, uEventType2, support);
		QueryResponseOrderedRelations qChain = this.queryPlanOrderedRelationsChain.getQueryResponseOrderedRelations();
		cChain.unpersist();
		joinedOrder.unpersist();
//		// combine all responses
		QueryResponseDeclareAll queryResponseAll = new QueryResponseDeclareAll(queryResponseExistence, queryResponsePosition, qSimple,
				qAlternate, qChain);
		return queryResponseAll;
	}
}
