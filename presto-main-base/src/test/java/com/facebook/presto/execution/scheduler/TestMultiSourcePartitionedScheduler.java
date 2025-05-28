/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.Session;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.dispatcher.NoOpQueryManager;
import com.facebook.presto.execution.MockRemoteTaskFactory;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.PartitionedSplitsInfo;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSelectorConfig;
import com.facebook.presto.failureDetector.NoOpFailureDetector;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.split.ConnectorAwareSplitSource;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.ttl.nodettlfetchermanagers.ThrowingNodeTtlFetcherManager;
import com.facebook.presto.util.FinalizerService;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.scheduler.ScheduleResult.BlockedReason.SPLIT_QUEUES_FULL;
import static com.facebook.presto.execution.scheduler.ScheduleResult.BlockedReason.WAITING_FOR_SOURCE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMultiSourcePartitionedScheduler
{
    public static final OutputBufferId OUT = new OutputBufferId(0);
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("connector_id");
    private static final PlanNodeId TABLE_SCAN_1_NODE_ID = new PlanNodeId("1");
    private static final PlanNodeId TABLE_SCAN_2_NODE_ID = new PlanNodeId("2");

    private final ExecutorService queryExecutor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("stageScheduledExecutor-%s"));
    private final InMemoryNodeManager nodeManager = new InMemoryNodeManager();
    private final FinalizerService finalizerService = new FinalizerService();
    private final Session session = TestingSession.testSessionBuilder().build();
    private final PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();

    public TestMultiSourcePartitionedScheduler()
    {
        nodeManager.addNode(CONNECTOR_ID,
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
    }

    @BeforeClass
    public void setUp()
    {
        finalizerService.start();
    }

    @AfterClass(alwaysRun = true)
    public void destroyExecutor()
    {
        queryExecutor.shutdownNow();
        scheduledExecutor.shutdownNow();
        finalizerService.destroy();
    }

    @Test
    public void testScheduleSplitsBatchedNoBlocking()
    {
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getMultiSourcePartitionedScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(60, TestingSplit::createRemoteSplit), TABLE_SCAN_2_NODE_ID, createFixedSplitSource(60, TestingSplit::createRemoteSplit)),
                stage,
                nodeManager,
                nodeTaskMap,
                7);

        for (int i = 0; i <= (60 / 7) * 2; i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            // finishes when last split is fetched
            if (i == (60 / 7) * 2) {
                assertEffectivelyFinished(scheduleResult, scheduler);
            }
            else {
                assertFalse(scheduleResult.isFinished());
            }

            // never blocks
            assertTrue(scheduleResult.getBlocked().isDone());

            // first three splits create new tasks
            assertEquals(scheduleResult.getNewTasks().size(), i == 0 ? 3 : 0);
        }

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 40);
        }

        stage.abort();
    }

    @Test
    public void testScheduleSplitsBatchedBlockingSplitSource()
    {
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);
        QueuedSplitSource blockingSplitSource = new QueuedSplitSource(TestingSplit::createRemoteSplit);

        StageScheduler scheduler = getMultiSourcePartitionedScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(10, TestingSplit::createRemoteSplit), TABLE_SCAN_2_NODE_ID, blockingSplitSource),
                stage,
                nodeManager,
                nodeTaskMap,
                5);

        ScheduleResult scheduleResult = scheduler.schedule();
        assertThat(scheduleResult.isFinished()).isFalse();
        assertThat(scheduleResult.getBlocked().isDone()).isTrue();
        assertThat(scheduleResult.getNewTasks()).hasSize(3);

        scheduleResult = scheduler.schedule();
        assertThat(scheduleResult.isFinished()).isFalse();
        assertThat(scheduleResult.getBlocked().isDone()).isFalse();
        assertThat(scheduleResult.getNewTasks()).isEmpty();
        assertThat(scheduleResult.getBlockedReason()).isEqualTo(Optional.of(WAITING_FOR_SOURCE));

        blockingSplitSource.addSplits(2, true);

        scheduleResult = scheduler.schedule();
        assertThat(scheduleResult.getBlocked().isDone()).isTrue();
        assertThat(scheduleResult.getSplitsScheduled()).isEqualTo(2);
        assertThat(scheduleResult.getNewTasks()).isEmpty();
        assertThat(scheduleResult.getBlockedReason()).isEqualTo(Optional.empty());
        assertThat(scheduleResult.isFinished()).isTrue();

        assertPartitionedSplitCount(stage, 12);
        assertEffectivelyFinished(scheduleResult, scheduler);

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertThat(splitsInfo.getCount()).isEqualTo(4);
        }
        stage.abort();
    }

    @Test
    public void testScheduleSplitsTasksAreFull()
    {
        // Test the case when tasks are full
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getMultiSourcePartitionedScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(200, TestingSplit::createRemoteSplit), TABLE_SCAN_2_NODE_ID, createFixedSplitSource(200, TestingSplit::createRemoteSplit)),
                stage,
                nodeManager,
                nodeTaskMap,
                200);

        ScheduleResult scheduleResult = scheduler.schedule();
        assertThat(scheduleResult.getSplitsScheduled()).isEqualTo(300);
        assertThat(scheduleResult.isFinished()).isFalse();
        assertThat(scheduleResult.getBlocked().isDone()).isFalse();
        assertThat(scheduleResult.getNewTasks()).hasSize(3);
        assertThat(scheduleResult.getBlockedReason()).isEqualTo(Optional.of(SPLIT_QUEUES_FULL));

        assertThat(stage.getAllTasks().stream().mapToInt(task -> task.getPartitionedSplitsInfo().getCount()).sum()).isEqualTo(300);
        stage.abort();
    }

    @Test
    public void testBalancedSplitAssignment()
    {
        // use private node manager so we can add a node later
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(CONNECTOR_ID,
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);

        // Schedule 15 splits - there are 3 nodes, each node should get 5 splits
        SubPlan firstPlan = createPlan();
        SqlStageExecution firstStage = createSqlStageExecution(firstPlan, nodeTaskMap);

        QueuedSplitSource firstSplitSource = new QueuedSplitSource(TestingSplit::createRemoteSplit);
        QueuedSplitSource secondSplitSource = new QueuedSplitSource(TestingSplit::createRemoteSplit);

        StageScheduler scheduler = getMultiSourcePartitionedScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, firstSplitSource, TABLE_SCAN_2_NODE_ID, secondSplitSource),
                firstStage,
                nodeManager,
                nodeTaskMap,
                15);

        // Only first split source produces splits at that moment
        firstSplitSource.addSplits(15, true);

        ScheduleResult scheduleResult = scheduler.schedule();
        assertFalse(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(firstStage.getAllTasks().size(), 3);
        for (RemoteTask remoteTask : firstStage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 5);
        }

        // Add new node
        InternalNode additionalNode = new InternalNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, additionalNode);

        // Second source produced splits now
        secondSplitSource.addSplits(3, true);

        scheduleResult = scheduler.schedule();

        assertEffectivelyFinished(scheduleResult, scheduler);
        assertThat(scheduleResult.getBlocked().isDone()).isTrue();
        assertThat(scheduleResult.isFinished()).isTrue();
        assertThat(scheduleResult.getNewTasks()).hasSize(1);
        assertThat(firstStage.getAllTasks()).hasSize(4);

        assertThat(firstStage.getAllTasks().get(0).getPartitionedSplitsInfo().getCount()).isEqualTo(5);
        assertThat(firstStage.getAllTasks().get(1).getPartitionedSplitsInfo().getCount()).isEqualTo(5);
        assertThat(firstStage.getAllTasks().get(2).getPartitionedSplitsInfo().getCount()).isEqualTo(5);
        assertThat(firstStage.getAllTasks().get(3).getPartitionedSplitsInfo().getCount()).isEqualTo(3);

        // Second source produces
        SubPlan secondPlan = createPlan();
        SqlStageExecution secondStage = createSqlStageExecution(secondPlan, nodeTaskMap);
        StageScheduler secondScheduler = getMultiSourcePartitionedScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(10, TestingSplit::createRemoteSplit), TABLE_SCAN_2_NODE_ID, createFixedSplitSource(10, TestingSplit::createRemoteSplit)),
                secondStage,
                nodeManager,
                nodeTaskMap,
                10);

        scheduleResult = secondScheduler.schedule();
        assertEffectivelyFinished(scheduleResult, secondScheduler);
        assertThat(scheduleResult.getBlocked().isDone()).isTrue();
        assertThat(scheduleResult.isFinished()).isTrue();
        assertThat(scheduleResult.getNewTasks()).hasSize(4);
        assertThat(secondStage.getAllTasks()).hasSize(4);

        for (RemoteTask task : secondStage.getAllTasks()) {
            assertThat(task.getPartitionedSplitsInfo().getCount()).isEqualTo(5);
        }
        firstStage.abort();
        secondStage.abort();
    }

    @Test
    public void testScheduleEmptySources()
    {
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getMultiSourcePartitionedScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(0, TestingSplit::createRemoteSplit), TABLE_SCAN_2_NODE_ID, createFixedSplitSource(0, TestingSplit::createRemoteSplit)),
                stage,
                nodeManager,
                nodeTaskMap,
                15);

        ScheduleResult scheduleResult = scheduler.schedule();

        // If both split sources produce no splits then internal schedulers add one split - it can be expected by some operators e.g. AggregationOperator
        assertThat(scheduleResult.getNewTasks()).hasSize(2);
        assertEffectivelyFinished(scheduleResult, scheduler);

        stage.abort();
    }

    @Test
    public void testNoNewTaskScheduledWhenChildStageBufferIsOverUtilized()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(CONNECTOR_ID,
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        SubPlan plan = createPlan();
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        // setting over utilized child output buffer
        StageScheduler scheduler = getMultiSourcePartitionedScheduler(
                ImmutableMap.of(TABLE_SCAN_1_NODE_ID, createFixedSplitSource(200, TestingSplit::createRemoteSplit), TABLE_SCAN_2_NODE_ID, createFixedSplitSource(200, TestingSplit::createRemoteSplit)),
                stage,
                nodeManager,
                nodeTaskMap,
                200);
        // the queues of 3 running nodes should be full
        ScheduleResult scheduleResult = scheduler.schedule();
        assertThat(scheduleResult.getBlockedReason()).isEqualTo(Optional.of(SPLIT_QUEUES_FULL));
        assertThat(scheduleResult.getNewTasks()).hasSize(3);
        assertThat(scheduleResult.getSplitsScheduled()).isEqualTo(300);
        for (RemoteTask remoteTask : scheduleResult.getNewTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertThat(splitsInfo.getCount()).isEqualTo(100);
        }

        // new node added but 1 child's output buffer is overutilized - so lockdown the tasks
        nodeManager.addNode(CONNECTOR_ID, new InternalNode("other4", URI.create("http://127.0.0.4:14"), NodeVersion.UNKNOWN, false));
        scheduleResult = scheduler.schedule();
        assertThat(scheduleResult.getBlockedReason()).isEqualTo(Optional.of(SPLIT_QUEUES_FULL));
        assertThat(scheduleResult.getNewTasks()).isEmpty();
        assertThat(scheduleResult.getSplitsScheduled()).isEqualTo(0);
    }

    private static void assertPartitionedSplitCount(SqlStageExecution stage, int expectedPartitionedSplitCount)
    {
        assertEquals(stage.getAllTasks().stream().mapToInt(remoteTask -> remoteTask.getPartitionedSplitsInfo().getCount()).sum(), expectedPartitionedSplitCount);
    }

    private static void assertEffectivelyFinished(ScheduleResult scheduleResult, StageScheduler scheduler)
    {
        if (scheduleResult.isFinished()) {
            assertTrue(scheduleResult.getBlocked().isDone());
            return;
        }

        assertTrue(scheduleResult.getBlocked().isDone());
        ScheduleResult nextScheduleResult = scheduler.schedule();
        assertTrue(nextScheduleResult.isFinished());
        assertTrue(nextScheduleResult.getBlocked().isDone());
        assertEquals(nextScheduleResult.getNewTasks().size(), 0);
        assertEquals(nextScheduleResult.getSplitsScheduled(), 0);
    }

    private static StageScheduler getMultiSourcePartitionedScheduler(
            Map<PlanNodeId, ConnectorSplitSource> splitSources,
            SqlStageExecution stage,
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            int splitBatchSize)
    {
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setIncludeCoordinator(false)
                .setMaxSplitsPerNode(100)
                .setMaxPendingSplitsPerTask(0);
        NodeScheduler nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                nodeManager,
                new NodeSelectionStats(),
                nodeSchedulerConfig,
                nodeTaskMap,
                new ThrowingNodeTtlFetcherManager(),
                new NoOpQueryManager(),
                new SimpleTtlNodeSelectorConfig());

        Map<PlanNodeId, SplitSource> sources = splitSources.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new ConnectorAwareSplitSource(CONNECTOR_ID, TestingTransactionHandle.create(), e.getValue())));

        SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(TestingSession.testSessionBuilder().build(), CONNECTOR_ID), stage::getAllTasks);
        return new MultiSourcePartitionedScheduler(stage, sources, placementPolicy, splitBatchSize, StageExecutionDescriptor.ungroupedExecution());
    }

    private SubPlan createPlan()
    {
        VariableReferenceExpression variable = new VariableReferenceExpression(Optional.empty(), "column", VARCHAR);

        // table scan with splitCount splits
        TableScanNode tableScanOne = new TableScanNode(
                Optional.empty(),
                TABLE_SCAN_1_NODE_ID,
                new TableHandle(CONNECTOR_ID, new TestingTableHandle(), TestingTransactionHandle.create(), Optional.empty()),
                ImmutableList.of(variable),
                ImmutableMap.of(variable, new TestingColumnHandle("column")),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        TableScanNode tableScanTwo = new TableScanNode(
                Optional.empty(),
                TABLE_SCAN_2_NODE_ID,
                new TableHandle(CONNECTOR_ID, new TestingTableHandle(), TestingTransactionHandle.create(), Optional.empty()),
                ImmutableList.of(variable),
                ImmutableMap.of(variable, new TestingColumnHandle("column")),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        RemoteSourceNode remote = new RemoteSourceNode(Optional.empty(), new PlanNodeId("remote_id"), new PlanFragmentId(0), ImmutableList.of(), false, Optional.empty(), GATHER);
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId(0),
                new JoinNode(
                        Optional.empty(),
                        new PlanNodeId("join_id"),
                        INNER,
                        new ExchangeNode(
                                Optional.empty(),
                                planNodeIdAllocator.getNextId(),
                                REPARTITION,
                                LOCAL,
                                new PartitioningScheme(
                                        Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()),
                                        tableScanOne.getOutputVariables()),
                                ImmutableList.of(
                                        tableScanOne,
                                        tableScanTwo),
                                ImmutableList.of(tableScanOne.getOutputVariables(), tableScanTwo.getOutputVariables()),
                                false,
                                Optional.empty()),
                        remote,
                        ImmutableList.of(),
                        ImmutableList.<VariableReferenceExpression>builder()
                                .addAll(tableScanOne.getOutputVariables())
                                .addAll(remote.getOutputVariables())
                                .build(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()),
                ImmutableSet.of(variable),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(TABLE_SCAN_1_NODE_ID, TABLE_SCAN_2_NODE_ID),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(variable)),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.of(StatsAndCosts.empty()),
                Optional.empty());

        return new SubPlan(testFragment, ImmutableList.of());
    }

    private static ConnectorSplitSource createFixedSplitSource(int splitCount, Supplier<ConnectorSplit> splitFactory)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (int i = 0; i < splitCount; i++) {
            splits.add(splitFactory.get());
        }
        return new FixedSplitSource(splits.build());
    }

    private SqlStageExecution createSqlStageExecution(SubPlan tableScanPlan, NodeTaskMap nodeTaskMap)
    {
        StageId stageId = new StageId(new QueryId("query"), 0);
        SqlStageExecution stage = SqlStageExecution.createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                tableScanPlan.getFragment(),
                new MockRemoteTaskFactory(queryExecutor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                queryExecutor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new TableWriteInfo(Optional.empty(), Optional.empty()));

        stage.setOutputBuffers(createInitialEmptyOutputBuffers(PARTITIONED)
                .withBuffer(OUT, 0)
                .withNoMoreBufferIds());

        return stage;
    }

    private static class QueuedSplitSource
            implements ConnectorSplitSource
    {
        private final Supplier<ConnectorSplit> splitFactory;
        private final LinkedBlockingQueue<ConnectorSplit> queue = new LinkedBlockingQueue<>();
        private CompletableFuture<?> notEmptyFuture = new CompletableFuture<>();
        private boolean closed;

        public QueuedSplitSource(Supplier<ConnectorSplit> splitFactory)
        {
            this.splitFactory = requireNonNull(splitFactory, "splitFactory is null");
        }

        synchronized void addSplits(int count, boolean lastSplits)
        {
            if (closed) {
                return;
            }
            for (int i = 0; i < count; i++) {
                queue.add(splitFactory.get());
            }
            if (lastSplits) {
                close();
            }
            notEmptyFuture.complete(null);
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
        {
            checkArgument(partitionHandle.equals(NOT_PARTITIONED), "partitionHandle must be NOT_PARTITIONED");
            return notEmptyFuture
                    .thenApply(x -> getBatch(maxSize))
                    .thenApply(splits -> new ConnectorSplitBatch(splits, isFinished()));
        }

        private synchronized List<ConnectorSplit> getBatch(int maxSize)
        {
            // take up to maxSize elements from the queue
            List<ConnectorSplit> elements = new ArrayList<>(maxSize);
            queue.drainTo(elements, maxSize);

            // if the queue is empty and the current future is finished, create a new one so
            // a new readers can be notified when the queue has elements to read
            if (queue.isEmpty() && !closed) {
                if (notEmptyFuture.isDone()) {
                    notEmptyFuture = new CompletableFuture<>();
                }
            }

            return ImmutableList.copyOf(elements);
        }

        @Override
        public synchronized boolean isFinished()
        {
            return closed && queue.isEmpty();
        }

        @Override
        public synchronized void close()
        {
            closed = true;
        }
    }
}
