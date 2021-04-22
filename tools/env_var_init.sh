#!/bin/bash
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo "Init environment variables"

echo "DYLD_LIBRARY_PATH = ${DYLD_LIBRARY_PATH}"
export DYLD_LIBRARY_PATH=/Users/runner/thirdparty/lib
echo "After export: DYLD_LIBRARY_PATH = ${DYLD_LIBRARY_PATH}"

echo "DYLD_LIBRARY_PATH=/Users/runner/thirdparty/lib" >> ~/.bash_profile
source ~/.bash_profile
echo "After source bash_profile: DYLD_LIBRARY_PATH = ${DYLD_LIBRARY_PATH}"

