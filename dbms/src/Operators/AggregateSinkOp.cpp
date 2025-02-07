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
#include <Operators/AggregateSinkOp.h>

namespace DB
{
OperatorStatus AggregateSinkOp::writeImpl(Block && block)
{
    if (unlikely(!block))
    {
        return OperatorStatus::FINISHED;
    }
    agg_context->executeOnBlock(index, block);
    total_rows += block.rows();
    block.clear();
    return OperatorStatus::NEED_INPUT;
}

void AggregateSinkOp::operateSuffix()
{
    LOG_DEBUG(log, "finish write with {} rows", total_rows);

    agg_context->writeSuffix();
}

} // namespace DB
