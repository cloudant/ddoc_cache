% Copyright 2014 Cloudant. All rights reserved.

-module(ddoc_cache_entry_sup).

-behaviour(gen_server).


-export([
    start_link/0,
    start_child/1,
    get_child/1,
    terminate_child/1,
    which_children/0,
    count_children/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).


-define(TIMEOUT, 10000).
-define(WAIT, 5000).


-record(entry, {key, pid, cleanup}).
-record(state, {index}).


-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% @doc - Starts a child with given Id and Module, Args
-spec start_child(term()) -> {ok, pid()} | {ok, 'ignore'} | {error, term()}.
start_child(ChildSpec) ->
    start_child(ChildSpec, []).


%% @doc - Starts a child with given Id and Module, Args
-spec start_child(term(), [any()]) ->
    {ok, pid()} |
    {ok, 'ignore'} |
    {error, term()}.
start_child({ChildID, MFA}, Options) ->
    check_options(Options),
    case get_child(ChildId) of
        not_found ->
            Msg = {start_child, ChildId, MFA, Options},
            gen_server:call(?MODULE, ChildSpec, ?TIMEOUT);
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
    Match = [{{entry,'$1','$2'},[],[{{'$1','$2'}}]}],
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


init([]) ->
    process_flag(trap_exit, true),
    Idx = ets:new(?MODULE, [named_table, public, {keypos, #entry.key}]),
    {ok, #state{index = Idx}}.


terminate(Reason, #state{index = Idx}) ->
    [shutdown_child(Idx, Child, Reason) || Child <- ets:tab2list(Idx)],
    ets:delete(Idx),
    ok.


handle_call({start_child, ChildId, MFA, Options}, _From, State) ->
    #state{index = Idx} = State,
    case ets:lookup(Idx, ChildId) of
        [] ->
            Reply = create_child(Idx, {ChildId, MFA}, Options),
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
    case ets:match_object(Idx, #entry{pid = Pid, _='_'}) of
        [#entry{key = Key} = Entry] ->
            ets:delete(Idx, Key),
            maybe_cleanup(Entry);
        [] ->
            ok
    end,
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.


code_change(_, State, _) ->
    {ok, State}.


create_child(Idx, {ChildId, {M, F, A}}, Options) ->
    case catch apply(M, F, A) of
        {ok, Pid} when is_pid(Pid) ->
            case erlang:is_process_alive(Pid) of
                true ->
                    ets:insert(Idx, make_entry(ChildId, Pid, Options)),
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


make_entry(ChildId, Pid, Options) ->
    Cleanup = case proplists:get_value(cleanup, Options) of
        {cleanup, CU} -> CU;
        undefined -> undefined
    end,
    #entry{
        key = ChildId,
        pid = Pid,
        cleanup = Cleanup
    }.


shutdown_child(Idx, Entry, Reason) ->
    try
        shutdown_child_int(Idx, Entry, Reason),
    after
        maybe_cleanup(Entry)
    end.


shutdown_child_int(Idx, #entry{key = ChildId, pid = Pid}, Reason) ->
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


maybe_cleanup(#entry{key = Key, pid = Pid, cleanup = {M, F, A}}) ->
    try
        erlang:apply(M, F, [Key, Pid | A]),
        ok
    catch _:_ ->
        ok
    end;
maybe_cleanup(_) ->
    ok.


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


check_options(Options) ->
    case proplists:get_value(cleanup, Options) of
        {cleanup, {M, F, A}} when is_atom(M), is_atom(F), is_list(A) ->
            ok;
        {cleanup, Else} ->
            erlang:error({badarg, {cleanup, Else}})
    end.


