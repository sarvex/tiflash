// Copyright 2023 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/Logger.h>
#include <Interpreters/Aggregator.h>
#include <Operators/Operator.h>

namespace DB
{
struct ThreadData
{
    size_t src_rows = 0;
    size_t src_bytes = 0;

    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;

    ThreadData(size_t keys_size, size_t aggregates_size)
    {
        key_columns.resize(keys_size);
        aggregate_columns.resize(aggregates_size);
    }
};

class AggregateContext
{
public:
    explicit AggregateContext(
        const String & req_id)
        : log(Logger::get(req_id))
    {
    }

    void initBuild(const Aggregator::Params & params, size_t max_threads_);

    void executeOnBlock(size_t task_index, const Block & block);

    void initConvergent();

    void writeSuffix();

    size_t getConvergentConcurrency();

    Block readForConvergent(size_t index);

    Block getHeader() const;

    bool useNullSource();

private:
    bool isTwoLevel();

private:
    std::unique_ptr<Aggregator> aggregator;
    bool keys_size = false;
    bool empty_result_for_aggregation_by_empty_set = false;

    std::atomic_bool inited_build = false;
    std::atomic_bool inited_convergent = false;

    MergingBucketsPtr merging_buckets;
    ManyAggregatedDataVariants many_data;
    std::vector<ThreadData> threads_data;
    size_t max_threads{};

    const LoggerPtr log;
};

using AggregateContextPtr = std::shared_ptr<AggregateContext>;
} // namespace DB
