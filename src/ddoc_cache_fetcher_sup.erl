% Copyright 2014 Cloudant. All rights reserved.

-module(ddoc_cache_fetcher_sup).

-behaviour(gen_server).

-export([start_link/1, start_child/1, get_child/1, terminate_child/1,
    which_children/0, count_children/0, get_revision/1, set_revision/2]).
%% gen_server's callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-define(TIMEOUT, 10000).
-define(WAIT, 5000).

-record(entry, {key, pid, rev}).
-record(state, {worker, index}).

-spec start_link(atom()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%% @doc - Starts a child with given Id and MFArgs
-spec start_child(term()) -> {ok, pid()} | {ok, 'ignore'} | {error, term()}.
start_child(ChildId) ->
    case get_child(ChildId) of
    not_found ->
        gen_server:call(?MODULE, {start_child, ChildId}, ?TIMEOUT);
    Else ->
        Else
    end.

%% @doc - Get a pid of a child with a given Id
-spec get_child(term()) -> {ok, pid()} | not_found | {error, term()}.
get_child(ChildId) ->
    try ets:lookup(?MODULE, ChildId) of
        [] ->
            not_found;
        [#entry{key = ChildId, pid = Pid}] ->
            {ok, Pid}
    catch
        error:badarg ->
            {error, {?MODULE, not_running}}
    end.

%% @doc - Kills the child with a given id
-spec terminate_child(term()) -> ok | {error, term()}.
terminate_child(ChildId) ->
    try ets:member(?MODULE, ChildId) of
        false -> ok;
        true -> gen_server:call(?MODULE, {terminate_child, ChildId}, ?TIMEOUT)
    catch
        error:badarg -> {error, {?MODULE, not_running}}
    end.

%% @doc - Returns a list of key, pid pairs for all active children
-spec which_children() -> [{term(), pid()}].
which_children() ->
    Match = [{{entry,'$1','$2','_'},[],[{{'$1','$2'}}]}],
    try ets:select(?MODULE, Match) of
        Result -> Result
    catch
        error:badarg -> {error, {?MODULE, not_running}}
    end.

%% @doc - Returns a number of active children
-spec count_children() -> non_neg_integer().
count_children() ->
    case ets:info(?MODULE, size) of
    undefined -> 0;
    N -> N
    end.

set_revision(Key, Rev) ->
    try ets:update_element(?MODULE, Key, {#entry.rev, Rev}) of
        true -> ok;
        false -> not_found
    catch
        error:badarg -> {error, {?MODULE, not_running}}
    end.

get_revision(Key) ->
    try ets:lookup(?MODULE, Key) of
        [#entry{key = Key, rev = Rev}] -> {ok, Rev};
        [] -> not_found
    catch
        error:badarg -> {error, {?MODULE, not_running}}
    end.


init(Module) ->
    process_flag(trap_exit, true),
    case ets:info(?MODULE) of
    undefined ->
        Idx = ets:new(?MODULE, [named_table, public, {keypos, #entry.key}]),
        {ok, #state{worker = Module, index = Idx}};
    _ ->
        {stop, ets_already_started}
    end.

handle_call({start_child, ChildId}, _From, State) ->
    #state{worker = Module, index = Idx} = State,
    case ets:lookup(Idx, ChildId) of
    [] ->
        ChildSpec = {ChildId, {Module, start_link, [ChildId]}},
        Reply = create_child(Idx, ChildSpec),
        {reply, Reply, State};
    [#entry{key = ChildId, pid = Pid}] ->
        {reply, {ok, Pid}, State}
  end;
handle_call({terminate_child, ChildId}, _From, State) ->
    #state{index = Idx} = State,
    case ets:lookup(Idx, ChildId) of
    [Child] -> shutdown_child(Idx, Child, shutdown);
    [] -> ok
    end,
    {reply, ok, State};
handle_call(Call, _From, State) ->
    {stop, {unknown_call, Call}, State}.

handle_cast(Cast, State) ->
    {stop, {unknown_cast, Cast}, State}.

handle_info({'EXIT', Pid, _Reason}, #state{index = Idx} = State) ->
    ets:match_delete(Idx, #entry{pid = Pid, _='_'}),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(Reason, #state{index = Idx}) ->
    [shutdown_child(Idx, Child, Reason) || Child <- ets:tab2list(Idx)],
    ets:delete(Idx),
    ok.

code_change(_, State, _) ->
    {ok, State}.

create_child(Idx, {ChildId, {M,F,A}}) ->
    case catch apply(M, F, A) of
    {ok, Pid} when is_pid(Pid) ->
        case erlang:is_process_alive(Pid) of
        true ->
            ets:insert(Idx, #entry{key = ChildId, pid = Pid}),
            {ok, Pid};
        false ->
            {ok, Pid}
        end;
    ignore ->
        {ok, ignore};
    {error, Err} ->
        {error, Err};
    Err ->
        {error, Err}
    end.

shutdown_child(Idx, #entry{key = ChildId, pid = Pid}, Reason) ->
    ets:delete(Idx, ChildId),
    case monitor_child(Pid) of
    ok ->
        exit(Pid, Reason),
        receive
        {'DOWN', _MRef, process, Pid, Reason} ->
            ok;
        {'DOWN', _MRef, process, Pid, OtherReason} ->
            {error, OtherReason}
        after ?WAIT ->
            exit(Pid, kill),
            receive
            {'DOWN', _MRef, process, Pid, killed} ->
                ok;
            {'DOWN', _MRef, process, Pid, OtherReason} ->
                {error, OtherReason}
            end
        end;
    {error, Err} ->
        {error, Err}
    end.

monitor_child(Pid) ->
    erlang:monitor(process, Pid),
    unlink(Pid),
    receive
    {'EXIT', Pid, Reason} ->
        receive
        {'DOWN', _, process, Pid, _} ->
            {error, Reason}
        end
    after 0 ->
        ok
    end.