% Copyright 2015 Cloudant. All rights reserved.

-module(ddoc_cache_keeper).

-behaviour(gen_server).
-vsn(1).

-export([start_link/0, init/1, handle_call/3, handle_cast/2,
    handle_info/2, terminate/2, code_change/3]).

-include("ddoc_cache.hrl").

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    ets:new(?CACHE, [set, named_table, public, {keypos, #entry.key}]),
    {ok, ok, hibernate}.

handle_call(_, _, State) ->
    {reply, ok, State, hibernate}.

handle_cast(_, State) ->
    {noreply, State, hibernate}.

handle_info(_, State) ->
    {noreply, State, hibernate}.

terminate(_Reason, _State) ->
    ets:delete(?CACHE).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
