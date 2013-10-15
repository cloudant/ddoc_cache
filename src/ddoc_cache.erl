% Copyright 2012 Cloudant. All rights reserved.

-module(ddoc_cache).

-include_lib("couch/include/couch_db.hrl").

-export([start/0, stop/0]).

% public API
-export([open_doc/2, open_doc/3, open_validation_funs/1, evict/2]).

% deprecated API
-export([open/2]).

start() ->
    application:start(ddoc_cache).

stop() ->
    application:stop(ddoc_cache).

open_doc(DbName, DocId) ->
    Key = {DbName, DocId, '_'},
    case ddoc_cache_opener:match_newest(Key) of
        {ok, _} = Resp ->
            margaret_counter:increment([ddoc_cache, hit]),
            Resp;
        missing ->
            margaret_counter:increment([ddoc_cache, miss]),
            ddoc_cache_opener:open_doc(DbName, DocId);
        recover ->
            margaret_counter:increment([ddoc_cache, recovery]),
            ddoc_cache_opener:recover_doc(DbName, DocId)
    end.

open_doc(DbName, DocId, RevId) ->
    Key = {DbName, DocId, RevId},
    case ddoc_cache_opener:lookup(Key) of
        {ok, _} = Resp ->
            margaret_counter:increment([ddoc_cache, hit]),
            Resp;
        missing ->
            margaret_counter:increment([ddoc_cache, miss]),
            ddoc_cache_opener:open_doc(DbName, DocId, RevId);
        recover ->
            margaret_counter:increment([ddoc_cache, recovery]),
            ddoc_cache_opener:recover_doc(DbName, DocId, RevId)
    end.

open_validation_funs(DbName) ->
    Key = {DbName, validation_funs},
    case ddoc_cache_opener:lookup(Key) of
        {ok, _} = Resp ->
            margaret_counter:increment([ddoc_cache, hit]),
            Resp;
        missing ->
            margaret_counter:increment([ddoc_cache, miss]),
            ddoc_cache_opener:open_validation_funs(DbName);
        recover ->
            margaret_counter:increment([ddoc_cache, recovery]),
            ddoc_cache_opener:recover_validation_funs(DbName)
    end.

evict(ShardDbName, DDocIds) ->
    DbName = mem3:dbname(ShardDbName),
    ddoc_cache_opener:evict_docs(DbName, DDocIds).

open(DbName, validation_funs) ->
    open_validation_funs(DbName);
open(DbName, DocId) ->
    open_doc(DbName, DocId).
