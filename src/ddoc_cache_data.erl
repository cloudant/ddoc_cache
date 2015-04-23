% Copyright 2015 Cloudant. All rights reserved.

-module(ddoc_cache_keeper).

-behaviour(gen_server).
-vsn(1).


-export([
    lookup/2,
    store/2,
    remove/1,
    remove_matches/1,
    member/1
]).

-export([
    start_link/0,
]).

-export([
    init/1,
    terminate/2, 
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-include("ddoc_cache.hrl").


-spec lookup(doc_key()) -> {ok, #doc{} | [term()]} | missing | recover.
lookup(Key) ->
    try ets:lookup(?CACHE, Key) of
        [#entry{key=Key, val=Val}] ->
            {ok, Val};
        [] ->
            missing
    catch
        error:badarg ->
            recover
    end.


-spec store(doc_key(), term()) -> ok.
store(Key, Value) ->
    true = ets:insert(?CACHE, #entry{key=Key, val=Doc}),
    ok.


-spec remove(doc_key()) -> ok.
remove(Key) ->
    true = ets:delete(?CACHE, Key),
    ok.


-spec remove_matches(doc_key()) -> ok.
remove_matches(KeyPattern) ->
    true = ets:match_delete(?CACHE, #entry{key=KeyPattern, _='_'}),
    ok.


-spec member(doc_key()) -> boolean().
member(Key) ->
    ets:member(?CACHE, Key).


-spec start_link() -> {ok, Pid}.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
    ets:new(?CACHE, [set, named_table, public, {keypos, #entry.key}]),
    {ok, ok, hibernate}.


terminate(_Reason, _State) ->
    true = ets:delete(?CACHE),
    ok.


handle_call(_, _, State) ->
    {reply, ok, State, hibernate}.


handle_cast(_, State) ->
    {noreply, State, hibernate}.


handle_info(_, State) ->
    {noreply, State, hibernate}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
