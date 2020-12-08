#!/usr/bin/env python
# -*- coding: utf-8 -*-
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

####################################################
# Sample script that shows how to save result data #
####################################################
import datetime
import os
import argparse
import csv
import json
import requests

# You need to enter the real URL and have the server running
DEFAULT_CODESPEED_URL = 'http://localhost:8000/'

current_date = datetime.datetime.today()

parser = argparse.ArgumentParser(description='Upload jmh benchmark csv results')
parser.add_argument('--commit', dest='commit', required=True,
                    help='md5')
parser.add_argument('--branch', dest='branch', required=True)
parser.add_argument('--input', dest='input', required=False,
                    help='input csv file')
parser.add_argument('--environment', dest='environment', required=True)
parser.add_argument('--dry', dest='dry', action='store_true')
parser.add_argument('--codespeed', dest='codespeed', default=DEFAULT_CODESPEED_URL,
                    help='codespeed url, default: %s' % DEFAULT_CODESPEED_URL)
parser.add_argument('--project', dest='project', default="Flink")
parser.add_argument('--exec', dest='executable', default="Flink")


def read_data(args):
    results = []
    if args.input:
        path = args.input
    else:
        path = "jmh-result.csv"
    modification_date = datetime.datetime.fromtimestamp(os.path.getmtime(path))

    with open(path) as csvFile:
        reader = csv.reader(csvFile, delimiter=",")
        lines = [line for line in reader]
        header = lines[0]
        params = sorted(filter(lambda s: s.startswith("Param"), header))
        param_indexes = [header.index(param) for param in params]
        benchmark_index = header.index("Benchmark")
        score_index = header.index("Score")
        error_index = score_index + 1

        for line in lines[1:]:
            name = line[benchmark_index].split(".")[-1]
            if len(param_indexes) > 0:
                for param_index in param_indexes:
                    if len(line[param_index]) > 0:
                        name += "." + line[param_index]

            results.append({
                'commitid': args.commit,
                'branch': args.branch,
                'project': args.project,
                'executable': args.executable,
                'benchmark': name,
                'environment': args.environment,
                'lessisbetter': False,
                'units': 'records/ms',
                'result_value': float(line[score_index]),

                'revision_date': str(modification_date),
                'result_date': str(modification_date),
                'std_dev': line[error_index],  # Optional. Default is blank
            })
    return results


def post_data(data, codespeed_url):
    try:
        r = requests.post(
            codespeed_url + 'result/add/json/', data=data)
    except Exception as e:
        print(str(e))
        return
    response = r.text
    print("Server (%s) response: %s\n" % (codespeed_url, response))


if __name__ == "__main__":
    args = parser.parse_args()

    data = json.dumps(read_data(args), indent=4, sort_keys=True)
    if args.dry:
        print(data)
    else:
        post_data({'json': data}, args.codespeed)
