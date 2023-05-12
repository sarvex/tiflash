#!/usr/bin/env python3
# Copyright 2022 PingCAP, Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import time

from sys import argv


if len(argv) < 4:
    print(f'Usage: {argv[0]} [database] [tables...] [MySQL client]')
    exit(1)

database = argv[1]
tables = argv[2:-1]
client = argv[-1]

timeout = 600
sleep_time = 1.0

for table in tables:
    if ',' in table:
        print(f'Find "," in {table}, please use " " as separator.')
        exit(1)

table_full_names = ', '.join(f'{database}.{table}' for table in tables)
print(f'=> wait for {table_full_names} available in TiFlash')

table_names = ', '.join(f"'{table}'" for table in tables)
query = f"select sum(available) from information_schema.tiflash_replica where table_schema='{database}' and table_name in ({table_names})"

start_time = time.time()

available = False
retry_count = 0
while True:
    for line in os.popen(f'{client} "{query}"').readlines():
        try:
            count = int(line.strip())
            if count == len(tables):
                available = True
                break
        except:
            pass

    if available:
        break

    retry_count += 1
    if retry_count % 10 == 0:
        print(f'=> waiting for {table_full_names} available')

    time_used = time.time() - start_time
    if time_used >= timeout:
        break
    else:
        # if it is near to timeout, sleep time will be shorter and then give it the last try.
        time.sleep(min(sleep_time, timeout - time_used))

time_used = time.time() - start_time

if available:
    print(f'=> all tables are available now. time = {time_used}s')
else:
    print(f'=> cannot sync tables in {time_used}s')
    exit(1)
