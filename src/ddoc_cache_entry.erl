% Copyright 2014 Cloudant. All rights reserved.

-module(ddoc_cache_entry).

-behaviour(gen_server).
-vsn(1).


-export([
    create/1,
    get_value/1
]).

-export([
    start_link/1,
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
-include_lib("couch/include/couch_db.hrl").


-record(st, {
    key,
    timeout = 60000
}).


-spec create(doc_key()) -> {ok, Pid}.
get(Key) ->
    try
        % create_child will return an existing child for
        % Key if one exists.
        MFA = {proc_lib, start_link, [?MODULE, init, [Key]]},
        {ok, Pid} = ddoc_cache_entry_sup:create_child(Key, MFA),
        case ddoc_cache_data:lookup(Key) of
            {ok, _} = Resp ->
                couch_stats:increment_counter([ddoc_cache, hit]),
                gen_server:cast(Pid, accessed);
            _ ->
                couch_stats:increment_counter([ddoc_cache, miss]),
                gen_server:call(Pid, get)
        end
    catch _:_ ->
        couch_stats:increment_counter([ddoc_cache, recovery]),
        ddoc_cache_util:open(Key)
    end.


-spec start_link(doc_key()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).


init(Key) ->
    proc_lib:init_ack({ok, self()}),
    {Pid, Ref} = erlang:spawn_monitor(fun() ->
        try
            exit(ddoc_cache_util:open(Key))
        catch 
            throw:Reason ->
                exit({throw, Reason});
            error:Reason ->
                exit({error, Reason});
            exit:Reason ->
                exit({exit, Reason})
        end
    end),
    receive
        {'DOWN', Ref, process, Pid, {ok, Val}} ->
            ddoc_cache_data:store(Key, Val),
            ddoc_cache_monitor:register(Key),
            gen_server:enter_loop(?MODULE, [], #st{key = Key});
        {'DOWN', Ref, process, Pid, Error} ->
            exit(Error)
    end.


terminate(Reason, #st{}) ->
    ok.


handle_call(get, From, St) ->
    {ok, _} = Resp = ddoc_cache_data:lookup(Key),
    {reply, Resp, St, St#st.timeout};

handle_call(Msg, _, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


handle_cast(accessed, St) ->
    {noreply, St, St#st.timeout};

handle_cast({set_timeout, TimeOut}, St) ->
    {noreply, St#st{timeout=TimeOut}, TimeOut};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info(timeout, St) ->
    case ddoc_cache_monitor:should_close() of
        true -> {stop, St};
        false -> {noreply, St, St#st.timeout}
    end;

handle_info(Msg, State) ->
    {stop, {invalid_info, Msg}, State}.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
