% Copyright 2012 Cloudant. All rights reserved.

-module(ddoc_cache_evictor).
-behaviour(gen_server).
-vsn(1).


-include("ddoc_cache.hrl").


-export([
    start_link/0
]).

-export([
    init/1,
    terminate/2,

    handle_call/3,
    handle_cast/2,
    handle_info/2,

    code_change/3
]).

-export([
    handle_db_event/3
]).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    {ok, Evictor} = couch_event:link_listener(
        ?MODULE, handle_db_event, nil, [all_dbs]
    ),
    {ok, undefined}.


terminate(_Reason, St) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


handle_cast({evict, DbName}, St) ->
    gen_server:abcast(mem3:nodes(), ?MODULE, {do_evict, DbName}),
    {noreply, St};

handle_cast({evict, DbName, DDocIds}, St) ->
    gen_server:abcast(mem3:nodes(), ?MODULE, {do_evict, DbName, DDocIds}),
    {noreply, St};

handle_cast({do_evict, DbName}, St) ->
    remove_match_docs({DbName, '_'}),
    remove_match_docs({DbName, '_', '_'}),
    {noreply, St};

handle_cast({do_evict, DbName, DDocIds}, St) ->
    ?MODULE:remove_match_docs({DbName, custom, '_'}),
    lists:foreach(fun(DDocId) ->
        remove_match_docs({DbName, DDocId, '_'}),
        case ddoc_cache_entry_sup:get_child({DbName, DDocId}) of
            {ok, Pid} ->
                ddoc_cache_entry:refresh(Pid);
            _ ->
                ok
        end
    end, DDocIds),
    {noreply, St};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info(Msg, St) ->
    {stop, {invalid_info, Msg}, St}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_db_event(ShardDbName, created, St) ->
    gen_server:cast(?MODULE, {evict, mem3:dbname(ShardDbName)}),
    {ok, St};

handle_db_event(ShardDbName, deleted, St) ->
    gen_server:cast(?MODULE, {evict, mem3:dbname(ShardDbName)}),
    {ok, St};

handle_db_event(_DbName, _Event, St) ->
    {ok, St}.


remove_doc(Key) ->
    true = ets:delete(?CACHE, Key).


remove_match_docs(KeyPattern) ->
    true = ets:match_delete(?CACHE, #entry{key=KeyPattern, _='_'}).
