/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.query.selection;

import com.linkedin.pinot.common.utils.JsonAssert;
import com.linkedin.pinot.util.TestUtils;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.query.MSelectionOrderByOperator;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV0;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.query.selection.SelectionOperatorService;
import com.linkedin.pinot.core.query.selection.SelectionOperatorUtils;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


public class TestSelectionQueries {

  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(FileUtils.getTempDirectory() + File.separator + "TestSelectionQueries");
  private static File INDEXES_DIR =
      new File(FileUtils.getTempDirectory() + File.separator + "TestSelectionQueriesList");

  public static IndexSegment _indexSegment;
  public Map<String, ImmutableDictionaryReader> _dictionaryMap;
  public Map<String, ColumnMetadata> _medataMap;

  private static List<IndexSegment> _indexSegmentList = new ArrayList<IndexSegment>();

  @BeforeClass
  public void setup() throws Exception {
    setupSegment();
  }

  @AfterClass
  public void tearDown() {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
  }

  private void setupSegment() throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "time_day",
            TimeUnit.DAYS, "test", "testTable");

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
    final File indexSegmentDir = new File(INDEX_DIR, driver.getSegmentName());
    _indexSegment = ColumnarSegmentLoader.load(indexSegmentDir, ReadMode.heap);
    _dictionaryMap = ((IndexSegmentImpl) _indexSegment).getDictionaryMap();
    _medataMap = ((SegmentMetadataImpl) ((IndexSegmentImpl) _indexSegment).getSegmentMetadata()).getColumnMetadataMap();
  }

  private void setupSegmentList(int numberOfSegments) throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));

    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    INDEXES_DIR.mkdir();

    for (int i = 0; i < numberOfSegments; ++i) {
      final File segmentDir = new File(INDEXES_DIR, "segment_" + i);

      final SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir, "time_day",
              TimeUnit.DAYS, "test", "testTable");

      final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
      driver.init(config);
      driver.build();

      System.out.println("built at : " + segmentDir.getAbsolutePath());
      _indexSegmentList.add(ColumnarSegmentLoader.load(new File(segmentDir, driver.getSegmentName()), ReadMode.heap));
    }
  }

  @Test
  public void testSelectionIteration() {
    final BReusableFilteredDocIdSetOperator docIdSetOperator =
        new BReusableFilteredDocIdSetOperator(null, _indexSegment.getTotalDocs(), 5000);
    final Map<String, DataSource> dataSourceMap = getDataSourceMap();

    final MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);

    final Selection selection = getSelectionQuery();

    final MSelectionOrderByOperator selectionOperator = new MSelectionOrderByOperator(_indexSegment, selection, projectionOperator);

    final IntermediateResultsBlock block = (IntermediateResultsBlock) selectionOperator.nextBlock();
    final PriorityQueue<Serializable[]> pq = (PriorityQueue<Serializable[]>) block.getSelectionResult();
    final DataSchema dataSchema = block.getSelectionDataSchema();
    System.out.println(dataSchema);
    int i = 0;
    while (!pq.isEmpty()) {
      final Serializable[] row = (Serializable[]) pq.poll();
      System.out.println(SelectionOperatorUtils.getRowStringFromSerializable(row, dataSchema));
      Assert.assertEquals(SelectionOperatorUtils.getRowStringFromSerializable(row, dataSchema),
          SELECTION_ITERATION_TEST_RESULTS[i++]);
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForSelectionNoFilter() throws Exception {
    final BrokerRequest brokerRequest = getSelectionNoFilterBrokerRequest();
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV0();
    final PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    final MSelectionOrderByOperator operator = (MSelectionOrderByOperator) rootPlanNode.run();
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    System.out.println("RunningTime : " + resultBlock.getTimeUsedMs());
    System.out.println("NumDocsScanned : " + resultBlock.getNumDocsScanned());
    System.out.println("TotalDocs : " + resultBlock.getTotalDocs());

    final SelectionOperatorService selectionOperatorService =
        new SelectionOperatorService(brokerRequest.getSelections(), resultBlock.getSelectionDataSchema());

    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:1111"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:2222"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:3333"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:4444"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:5555"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:6666"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:7777"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:8888"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:9999"), resultBlock.getDataTable());
    final Collection<Serializable[]> reducedResults = selectionOperatorService.reduce(instanceResponseMap);
    final JSONObject jsonResult = selectionOperatorService.render(reducedResults);
    System.out.println(jsonResult);
    JsonAssert
        .assertEqualsIgnoreOrder(
            jsonResult.toString(),
            "{\"results\":[[\"u\",\"71\",\"1\"],[\"u\",\"93\",\"1\"],[\"u\",\"96\",\"3\"],[\"u\",\"90\",\"1\"],[\"u\",\"67\",\"1\"],[\"u\",\"48\",\"2\"],[\"u\",\"27\",\"1\"],[\"u\",\"12\",\"2\"],[\"u\",\"80\",\"2\"],[\"u\",\"68\",\"3\"]],\"columns\":[\"dim_memberGender\",\"dim_memberIndustry\",\"met_impressionCount\"]}");

  }

  @Test
  public void testInnerSegmentPlanMakerForSelectionWithFilter() throws Exception {
    final BrokerRequest brokerRequest = getSelectionWithFilterBrokerRequest();
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV0();
    final PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    final MSelectionOrderByOperator operator = (MSelectionOrderByOperator) rootPlanNode.run();
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    System.out.println("RunningTime : " + resultBlock.getTimeUsedMs());
    System.out.println("NumDocsScanned : " + resultBlock.getNumDocsScanned());
    System.out.println("TotalDocs : " + resultBlock.getTotalDocs());
    Assert.assertEquals(resultBlock.getNumDocsScanned(), 582);
    Assert.assertEquals(resultBlock.getTotalDocs(), 10001);

    final SelectionOperatorService selectionOperatorService =
        new SelectionOperatorService(brokerRequest.getSelections(), resultBlock.getSelectionDataSchema());

    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:1111"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:2222"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:3333"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:4444"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:5555"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:6666"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:7777"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:8888"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:9999"), resultBlock.getDataTable());
    final Collection<Serializable[]> reducedResults = selectionOperatorService.reduce(instanceResponseMap);
    final JSONObject jsonResult = selectionOperatorService.render(reducedResults);
    System.out.println(jsonResult);
    JsonAssert
        .assertEqualsIgnoreOrder(
            jsonResult.toString(),
            "{\"results\":[[\"u\",\"68\",\"3\"],[\"u\",\"12\",\"2\"],[\"u\",\"90\",\"1\"],[\"u\",\"67\",\"1\"],[\"u\",\"80\",\"2\"],[\"u\",\"48\",\"2\"],[\"u\",\"71\",\"1\"],[\"u\",\"27\",\"1\"],[\"u\",\"93\",\"1\"],[\"u\",\"96\",\"3\"]],\"columns\":[\"dim_memberGender\",\"dim_memberIndustry\",\"met_impressionCount\"]}");
  }

  @Test
  public void testInterSegmentSelectionPlanMakerAndRun() throws Exception {
    final int numSegments = 20;
    setupSegmentList(numSegments);
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    final BrokerRequest brokerRequest = getSelectionNoFilterBrokerRequest();
    final ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    final Plan globalPlan =
        instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService, 150000);
    globalPlan.print();
    globalPlan.execute();
    final DataTable instanceResponse = globalPlan.getInstanceResponse();
    System.out.println("instanceResponse : " + instanceResponse);

    final DefaultReduceService defaultReduceService = new DefaultReduceService();
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponse brokerResponse = defaultReduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    System.out.println("Selection Result : " + brokerResponse.getSelectionResults());
    System.out.println("Time used : " + brokerResponse.getTimeUsedMs());
    JsonAssert
        .assertEqualsIgnoreOrder(
            brokerResponse.getSelectionResults().toString(),
            "{\"results\":[[\"u\",\"71\",\"1\"],[\"u\",\"93\",\"1\"],[\"u\",\"96\",\"3\"],[\"u\",\"90\",\"1\"],[\"u\",\"67\",\"1\"],[\"u\",\"48\",\"2\"],[\"u\",\"27\",\"1\"],[\"u\",\"12\",\"2\"],[\"u\",\"80\",\"2\"],[\"u\",\"68\",\"3\"]],\"columns\":[\"dim_memberGender\",\"dim_memberIndustry\",\"met_impressionCount\"]}");
  }

  private static Map<String, DataSource> getDataSourceMap() {
    final Map<String, DataSource> dataSourceMap = new HashMap<String, DataSource>();
    dataSourceMap.put("dim_memberGender", _indexSegment.getDataSource("dim_memberGender"));
    dataSourceMap.put("dim_memberIndustry", _indexSegment.getDataSource("dim_memberIndustry"));
    dataSourceMap.put("met_impressionCount", _indexSegment.getDataSource("met_impressionCount"));
    return dataSourceMap;
  }

  private BrokerRequest getSelectionNoFilterBrokerRequest() {
    final BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(getSelectionQuery());
    return brokerRequest;
  }

  private BrokerRequest getSelectionWithFilterBrokerRequest() {
    final BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(getSelectionQuery());
    setFilterQuery(brokerRequest);
    return brokerRequest;
  }

  private static BrokerRequest setFilterQuery(BrokerRequest brokerRequest) {
    FilterQueryTree filterQueryTree;
    final String filterColumn = "dim_memberGender";
    final String filterVal = "u";
    if (filterColumn.contains(",")) {
      final String[] filterColumns = filterColumn.split(",");
      final String[] filterValues = filterVal.split(",");
      final List<FilterQueryTree> nested = new ArrayList<FilterQueryTree>();
      for (int i = 0; i < filterColumns.length; i++) {

        final List<String> vals = new ArrayList<String>();
        vals.add(filterValues[i]);
        final FilterQueryTree d = new FilterQueryTree(i + 1, filterColumns[i], vals, FilterOperator.EQUALITY, null);
        nested.add(d);
      }
      filterQueryTree = new FilterQueryTree(0, null, null, FilterOperator.AND, nested);
    } else {
      final List<String> vals = new ArrayList<String>();
      vals.add(filterVal);
      filterQueryTree = new FilterQueryTree(0, filterColumn, vals, FilterOperator.EQUALITY, null);
    }
    RequestUtils.generateFilterFromTree(filterQueryTree, brokerRequest);
    return brokerRequest;
  }

  private Selection getSelectionQuery() {
    final Selection selection = new Selection();
    final List<String> selectionColumns = new ArrayList<String>();
    selectionColumns.add("dim_memberGender");
    selectionColumns.add("dim_memberIndustry");
    selectionColumns.add("met_impressionCount");
    selection.setSelectionColumns(selectionColumns);
    selection.setOffset(0);
    selection.setSize(10);
    final List<SelectionSort> selectionSortSequence = new ArrayList<SelectionSort>();
    final SelectionSort selectionSort = new SelectionSort();
    selectionSort.setColumn("dim_memberGender");
    selectionSort.setIsAsc(false);
    selectionSortSequence.add(selectionSort);
    selection.setSelectionSortSequence(selectionSortSequence);
    return selection;
  }

  private static String[] SELECTION_ITERATION_TEST_RESULTS =
      new String[] { "u : 68 : 3", "u : 80 : 2", "u : 12 : 2", "u : 27 : 1", "u : 48 : 2", "u : 67 : 1", "u : 90 : 1", "u : 96 : 3", "u : 93 : 1", "u : 71 : 1" };
}
