-module(dummy).


-behaviour(gen_server).


-export([
    start_link/1
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-record(ctx, {key, delay}).


start_link(Key) ->
    gen_server:start_link({local, Key}, ?MODULE, Key, []).


init(Key) ->
    {ok, #ctx{key = Key}}.


terminate(_Reason, _Ctx) ->
    ok.


handle_call(crash, _From, Ctx) ->
    erlang:error(crowbar),
    {reply, ok, Ctx};

handle_call({delay, Delay}, _From, Ctx) ->
    {reply, ok, Ctx#ctx{delay = Delay}, Delay}.


handle_cast(_, Ctx) ->
    {stop, {error, unknown_call}, Ctx}.


handle_info(timeout, Ctx) ->
    {stop, normal, Ctx};

handle_info(_, Ctx) ->
    {noreply, Ctx}.


code_change(_OldVsn, Ctx, _Extra) ->
    {ok, Ctx}.
