/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.thriftserver.session;

import java.io.Serializable;
import java.util.*;

import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction2;

import org.apache.spark.api.java.JavaRDD;

class PartitionSampleFunction<T> extends AbstractFunction2<org.apache.spark.TaskContext, scala.collection.Iterator<T>, List<T>>
        implements Serializable {
    private Map<Integer, SamplePartition> samplePartitionMap;

    PartitionSampleFunction(Map<Integer, SamplePartition> samplePartitionMap) {
        this.samplePartitionMap = samplePartitionMap;
    }

    @Override
    public List<T> apply(org.apache.spark.TaskContext tc, scala.collection.Iterator<T> iterator) {
        List<T> list = new ArrayList<>();
        int index = 0;
        T element = null;
        SamplePartition sp = samplePartitionMap.get(tc.partitionId());
        while (iterator.hasNext() && index < sp.getEnd()) {
            element = iterator.next();
            if (index >= sp.getStart() && index < sp.getEnd()) {
                list.add(element);
            }
            index++;
        }

        return list;
    }
}

/**
 *  Record the next position in partition waiting to fetched by runJob.
 *  For example, if PartitionCursor is (1, 2),
 *  it means runJob should get elements from the third element of the second partition
 */
class PartitionCursor {
    private Integer partitionIndex;
    private Integer nextIndexInPartition;

    public PartitionCursor(Integer partitionIndex, Integer nextIndexInPartition) {
        this.partitionIndex = partitionIndex;
        this.nextIndexInPartition = nextIndexInPartition;
    }

    public Integer getPartitionIndex() {
        return partitionIndex;
    }

    public Integer getNextIndexInPartition() {
        return nextIndexInPartition;
    }
}

/**
 *  Get elements in [start, end) of Partition: partitionId
 */
class SamplePartition implements Serializable {
    private Integer partitionId;
    private Integer start;
    private Integer end;

    public SamplePartition(Integer partitionId, Integer startIndex, Integer endIndex) {
        this.partitionId = partitionId;
        this.start = startIndex;
        this.end = endIndex;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public Integer getStart() {
        return start;
    }

    public Integer getEnd() {
        return end;
    }
}

public class RDDStreamIterator<T> implements Iterator<T> {
    private JavaRDD<T> rdd;
    private Integer batchSize;

    private PartitionCursor partitionCursor;
    private List<Integer> partitionSizeList;
    private Iterator<T> iter;
    // the num of elements in rdd
    private Long totalItemNum;
    // the num of elements return by next()
    private Long currItemNum;

    public RDDStreamIterator(JavaRDD<T> rdd, Integer batchSize) {
        this.rdd = rdd;
        this.batchSize = batchSize;
        partitionCursor = new PartitionCursor(0, 0);
        this.partitionSizeList = null;
        iter = (new ArrayList<T>()).iterator();
        this.totalItemNum = 0L;
        this.currItemNum = 0L;

    }

    private void collectPartitionSize() {
        if(this.partitionSizeList == null) {
            this.partitionSizeList = this.rdd.mapPartitions(iter -> {
                int count = 0;
                while(iter.hasNext()) {
                    iter.next();
                    count ++;
                }
                return Collections.singleton(count).iterator();
            }).collect();

            for (int i = 0; i < this.partitionSizeList.size(); i ++) {
                this.totalItemNum = this.totalItemNum + this.partitionSizeList.get(i);
            }
        }
    }

    private Iterator<T> collectPartitionByBatch(Map<Integer, SamplePartition> samplePartitionMap) {
        List<T>[] batches = (List<T>[])rdd.context().runJob(rdd.rdd(),
                new PartitionSampleFunction<T>(samplePartitionMap),
                (scala.collection.Seq) JavaConversions.asScalaBuffer(new ArrayList<Integer>(samplePartitionMap.keySet())),
                scala.reflect.ClassTag$.MODULE$.apply(List.class));

        List<T> result = new ArrayList<T>();
        for (int i = 0; i < batches.length; i ++) {
            result.addAll(batches[i]);
        }

        return result.iterator();
    }

    private boolean isIndexOutOfBound() {
        if(this.partitionSizeList == null) {
            collectPartitionSize();
        }
        return currItemNum >= totalItemNum;
    }

    public boolean hasNext() {
        if (iter.hasNext()) {
            return true;
        }

        if (!isIndexOutOfBound()) {
            return true;
        }

        return false;
    }

    /**
     * For example:
     *   There are 3 partitions with size: 3, 2, 5 and the partitionCursor is (1, 1), and batchSize is 4.
     *   So get the last element from the second partition and get the 3 elements from the third partition.
     *   So the return map is {1: SamplePartition{1, 1, 2}, 2: SamplePartition{2, 0, 3}}.
     *   And partitionCursor move to (2, 3)
     */
    private Map<Integer, SamplePartition> getSamplePartitions() {
        Map<Integer, SamplePartition> samplePartitionMap = new TreeMap<>();
        int leftBatchSize = batchSize;
        int startIndex = partitionCursor.getNextIndexInPartition();

        for (int i = partitionCursor.getPartitionIndex(); i < partitionSizeList.size(); i ++) {
            int partitionSize = partitionSizeList.get(i);

            int sizeInCurPartition = partitionSize;
            if (i == partitionCursor.getPartitionIndex()) {
                sizeInCurPartition -= startIndex;
            }

            if (leftBatchSize > sizeInCurPartition) {
                leftBatchSize = leftBatchSize - sizeInCurPartition;
                SamplePartition sp = new SamplePartition(i, startIndex, partitionSize);

                samplePartitionMap.put(sp.getPartitionId(), sp);
                startIndex = 0;

                continue;
            } else {
                SamplePartition sp = new SamplePartition(i, startIndex, leftBatchSize);
                samplePartitionMap.put(sp.getPartitionId(), sp);

                if (leftBatchSize == sizeInCurPartition) {
                    partitionCursor = new PartitionCursor(i + 1, 0);
                } else if (leftBatchSize < sizeInCurPartition) {
                    partitionCursor = new PartitionCursor(i, leftBatchSize);
                }

                return samplePartitionMap;
            }
        }

        partitionCursor = new PartitionCursor(partitionSizeList.size(), 0);
        return samplePartitionMap;
    }

    public T next() {
        if (iter.hasNext()) {
            currItemNum ++;
            return iter.next();
        }

        if (isIndexOutOfBound()) {
            throw new NoSuchElementException();
        }

        iter = collectPartitionByBatch(getSamplePartitions());

        currItemNum ++;
        return iter.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
