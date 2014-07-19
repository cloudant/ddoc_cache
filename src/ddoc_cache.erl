% Copyright 2012 Cloudant. All rights reserved.

-module(ddoc_cache).

-export([
    start/0,
    stop/0
]).

-export([
    open_doc/2,
    open_doc/3,
    open_validation_funs/1,
    evict/2,

    %% deprecated
    open/2
]).

start() ->
    application:start(ddoc_cache).

stop() ->
    application:stop(ddoc_cache).

open_doc(DbName, DocId) ->
    Key = {DbName, DocId, '_'},
    case ddoc_cache_opener:match_newest(Key) of
        {ok, _} = Resp ->
            couch_stats:increment_counter([ddoc_cache, hit]),
            Resp;
        missing ->
            couch_stats:increment_counter([ddoc_cache, miss]),
            ddoc_cache_opener:open_doc(DbName, DocId);
        recover ->
            couch_stats:increment_counter([ddoc_cache, recovery]),
            ddoc_cache_opener:recover_doc(DbName, DocId)
    end.

open_doc(DbName, DocId, RevId) ->
    Key = {DbName, DocId, RevId},
    case ddoc_cache_opener:lookup(Key) of
        {ok, _} = Resp ->
            couch_stats:increment_counter([ddoc_cache, hit]),
            Resp;
        missing ->
            couch_stats:increment_counter([ddoc_cache, miss]),
            ddoc_cache_opener:open_doc(DbName, DocId, RevId);
        recover ->
            couch_stats:increment_counter([ddoc_cache, recovery]),
            ddoc_cache_opener:recover_doc(DbName, DocId, RevId)
    end.

open_validation_funs(DbName) ->
    Key = {DbName, validation_funs},
    case ddoc_cache_opener:lookup(Key) of
        {ok, _} = Resp ->
            couch_stats:increment_counter([ddoc_cache, hit]),
            Resp;
        missing ->
            couch_stats:increment_counter([ddoc_cache, miss]),
            ddoc_cache_opener:open_validation_funs(DbName);
        recover ->
            couch_stats:increment_counter([ddoc_cache, recovery]),
            ddoc_cache_opener:recover_validation_funs(DbName)
    end.

evict(ShardDbName, DDocIds) ->
    DbName = mem3:dbname(ShardDbName),
    ddoc_cache_opener:evict_docs(DbName, DDocIds).

open(DbName, validation_funs) ->
    open_validation_funs(DbName);
open(DbName, <<"_design/", _/binary>>=DDocId) when is_binary(DbName) ->
    open_doc(DbName, DDocId);
open(DbName, DDocId) when is_binary(DDocId) ->
    open_doc(DbName, <<"_design/", DDocId/binary>>).
