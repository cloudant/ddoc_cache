% Copyright 2014 Cloudant. All rights reserved.

-module(ddoc_cache_monitor).

-behaviour(gen_server).
-behaviour(config_listener).
-vsn(1).


-export([
    register/1,
    should_close/0
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
    handle_config_change/5
]).


-record(st, {
    max_objects,
    max_memory
}).


-spec register(doc_key()) -> ok.
register(Key) ->
    gen_server:cast(?MODULE, {register, self(), Key})


-spec should_close() -> true | false.
should_close() ->
    gen_server:call(?MODULE, should_close).


-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link(?MODULE, [], []).


init(_) ->
    ets:new(?MODULE, [set, named_table, public]),
    ok = config:listen_for_changes(?MODULE, self()),
    {ok, load_config(#st{})},


terminate(_Reason, _St) ->
    ok.


handle_call(should_close, _From, St) ->
    #st{max_objects = MaxObj, max_memory = MaxMem} = St,
    ObjExceeded = MaxObj > 0 andalso ets:info(?CACHE, size) > MaxObj,
    MemExceeded = ets:info(?CACHE, memory) > MaxMem,
    {reply, ObjExceeded or MemExceeded, St};

handle_call(Msg, _, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


handle_cast({register, Pid, Key}, St) ->
    Ref = erlang:monitor(process, Pid),
    ets:insert(?MODULE, {Ref, Pid, Key}),
    {noreply, St};

handle_cast(reload_config, St) ->
    {noreply, load_config(St)};

handle_cast(set_timeout, St) ->
    TimeOut = config:get_integer("ddoc_cache", "timeout", 60000),
    ets:foldl(fun({_Ref, Pid, _Key}, _Acc) ->
        gen_server:cast(Pid, {set_timeout, TimeOut})
    end, nil, ?MODULE),
    {noreply, St};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info({'DOWN', Ref, process, _Pid, _Reason}, St) ->
    case ets:lookup(?MODULE, Ref) of
        {Ref, _Pid, Key} ->
            ddoc_cache_data:remove(Key);
        _ ->
            ok
    end,
    ets:delete(?MODULE, Ref),
    {noreply, St};

handle_info(Msg, State) ->
    {stop, {invalid_info, Msg}, State}.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


handle_config_change("ddoc_cache", Key, _, _, Pid) ->
    Cmd = case Key of
        "max_objects" -> reload_config;
        "max_size" -> reload_config
        "timeout" -> set_timeout
    end,
    gen_server:cast(Pid, Cmd),
    {ok, Pid};

handle_config_change(_, _, _, _, Pid)
    {ok, Pid}.


load_config(St) ->
    St#st{
        max_objects = config:get_integer("ddoc_cache", "max_objects", 0),
        max_size = config:get_integer("ddoc_cache", "max_size", 104857600)
    }.
