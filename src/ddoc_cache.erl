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
    ddoc_cache_entry:get({DbName, DocId}).


open_doc(DbName, DocId, RevId) ->
    ddoc_cache_entry:get({DbName, DocId, RevId}).


open_validation_funs(DbName) ->
    ddoc_cache_entry:get({DbName, custom, validation_funs}).


open_custom(DbName, Mod) ->
    ddoc_cache_entry:get({DbName, custom, Mod}).


evict(ShardDbName, DDocIds) ->
    DbName = mem3:dbname(ShardDbName),
    gen_server:cast(ddoc_cache_evictor, {evict, DbName, DDocIds}).


open(DbName, validation_funs) ->
    open_validation_funs(DbName);
open(DbName, Module) when is_atom(Module) ->
    open_custom(DbName, Module);
open(DbName, <<"_design/", _/binary>>=DDocId) when is_binary(DbName) ->
    open_doc(DbName, DDocId);
open(DbName, DDocId) when is_binary(DDocId) ->
    open_doc(DbName, <<"_design/", DDocId/binary>>).
