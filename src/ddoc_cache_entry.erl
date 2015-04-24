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

-export([
    cleanup/2,
    refresh/2
]).


-include("ddoc_cache.hrl").
-include_lib("couch/include/couch_db.hrl").


-record(st, {
    key,
    max_objects = 0,
    max_memory = 104857600,
    timeout = 60000
}).


-spec create(doc_key()) -> {ok, Pid}.
get(Key) ->
    try
        % create_child will return an existing child for
        % Key if one exists.
        MFA = {proc_lib, start_link, [?MODULE, init, [Key]]},
        Opts = [{cleanup, {?MODULE, cleanup, []}}],
        {ok, Pid} = ddoc_cache_entry_sup:create_child({Key, MFA}, Opts),
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

handle_cast({config, Config}, St) ->
    NewSt = lists:foldl(fun
        ({max_objects, MaxObj}, Acc) ->
            Acc#st{max_objects = MaxObj};
        ({max_memory, MaxMem}, Acc) ->
            Acc#st{max_memory = MaxMem};
        ({timeout, TimeOut}, Acc) ->
            Acc#st{timeout = TimeOut}
        (_Else, Acc) ->
            Acc
    end, St, Config)
    {noreply, NewSt, NewSt#st.timeout};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info(timeout, St) ->
    case should_close(St) of
        true ->
            {stop, St};
        false ->
            NewSt = maybe_refresh(St),
            {noreply, NewSt, NewSt#st.timeout}
    end;

handle_info(Msg, State) ->
    {stop, {invalid_info, Msg}, State}.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


cleanup(Key, _Pid) ->
    ddoc_cache_data:remove(Key).


should_close(St) ->
    #st{max_objects = MaxObj, max_memory = MaxMem} = St,
    ObjExceeded = MaxObj > 0 andalso ets:info(?CACHE, size) > MaxObj,
    MemExceeded = ets:info(?CACHE, memory) > MaxMem,
    ObjExceeded orelse MemExceeded.


maybe_refresh(#st{key = {DbName, DocId}} = St) ->
    proc_lib:spawn_link(?MODULE, refresh, [{DbName, DocId}, self()]);
    St;

maybe_refresh(St) ->
    St.


refresh(Key, Server) ->
    case ddoc_cache_util:open(Key) of
        {ok, _} = Resp ->
            gen_server:cast(Server, Resp);
        Error ->
            exit(Error)
    end.
