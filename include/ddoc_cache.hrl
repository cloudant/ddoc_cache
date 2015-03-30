% Copyright 2015 Cloudant
%
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

%% types
-type db_name() :: iodata().
-type doc_id() :: iodata().
-type doc_hash() :: <<_:128>>.
-type revision() :: {pos_integer(), doc_hash()}.
-type doc_key() :: {db_name(), doc_id()}
    | {db_name(), 'custom', atom()}
    | {db_name(), doc_id(), revision()}.

% cache record
-define(CACHE, ddoc_cache_lru).
-record(entry, {
    key :: doc_key() | tuple(),
    val :: binary() | '_',
    ts :: pos_integer() | atom() | '_'
}).
