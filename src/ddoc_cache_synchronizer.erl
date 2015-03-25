% Copyright 2014 Cloudant. All rights reserved.

-module(ddoc_cache_synchronizer).

-behaviour(gen_server).
-vsn(1).

-export([start_link/1, start/2, refresh/1, stop/1]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2,
    handle_info/2, terminate/2, code_change/3]).

-include("ddoc_cache.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(CACHE, ddoc_cache_lru).
-define(REFRESH, 60000).
-record(state, {key, rev, pid, tref}).


-spec start_link(doc_key()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).

-spec start(doc_key(), revision()) -> {ok, pid()}
    | {ok, 'ignore'}  | {error, term()}.
start(Key, Rev) ->
    ChildSpec = {{synchronizer, Key}, ?MODULE, [{Key, Rev}]},
    ddoc_cache_fetcher_sup:start_child(ChildSpec).

-spec refresh(doc_key()) -> ok.
refresh(Key) ->
    case ddoc_cache_fetcher_sup:get_child({synchronizer, Key}) of
    {ok, Pid} ->
        gen_server:cast(Pid, refresh);
    not_found ->
        ok
    end.

-spec stop(doc_key()) -> ok | {error, term()}.
stop(Key) ->
    ddoc_cache_fetcher_sup:terminate_child({synchronizer, Key}).


init({Key, Rev}) ->
    process_flag(trap_exit, true),
    {ok, TRef} = set_timer(?REFRESH),
    {ok, #state{key = Key, rev = Rev, tref = TRef}, hibernate}.

handle_call(Msg, _, State) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, State}.

handle_cast(refresh, #state{tref = undefined} = State) ->
    {noreply, State};
handle_cast(refresh, #state{key = {DbName, DocId}, rev = Rev} = State) ->
    {ok, cancel} = timer:cancel(State#state.tref),
    case ets_lru:member(?CACHE, {DbName, DocId, Rev}) of
    true ->
        Self = self(),
        Pid = proc_lib:spawn_link(fun() ->
            erlang:apply(fun recover/2, [Self, {DbName, DocId}])
        end),
        {noreply, State#state{tref = undefined, pid = Pid}};
    false ->
        {stop, normal, State#state{tref = undefined}}
    end;
handle_cast({recover, {ok, DocInfo}}, #state{rev = Rev} = State) ->
    #doc_info{revs = RecRevs} = DocInfo,
    Revs = [R || #rev_info{rev=R, deleted=D} <- RecRevs, D /= true],
    case lists:sort(fun({A,_}, {B,_}) -> A > B end, Revs) of
    [Rev|_] ->
        {noreply, State};
    _ ->
        {DbName, DocId} = State#state.key,
        ddoc_cache_opener:evict_docs(DbName, [DocId]),
        {stop, normal, State#state{tref = undefined}}
    end;
handle_cast({recover, Reply}, #state{key = Key} = State) ->
    twig:log(notice, "Can't get doc info for ~p : ~w", [Key, Reply]),
    {noreply, State};
handle_cast(Msg, State) ->
    {stop, {invalid_cast, Msg}, State}.

handle_info({'EXIT', Pid, normal}, #state{pid=Pid} = State) ->
    {ok, TRef} = set_timer(?REFRESH),
    {noreply, State#state{pid = undefined, tref = TRef}, hibernate};
handle_info({'EXIT', Pid, Reason}, #state{pid=Pid} = State) ->
    {stop, Reason, State#state{pid = undefined}};
handle_info(Msg, State) ->
    {stop, {invalid_info, Msg}, State}.

terminate(Reason, #state{tref = undefined, pid = Pid}) ->
    case is_pid(Pid) andalso erlang:is_process_alive(Pid) of
    true -> exit(Pid, Reason);
    false -> ok
    end;
terminate(Reason, #state{tref = TRef} = State) ->
    {ok, cancel} = timer:cancel(TRef),
    terminate(Reason, State#state{tref = undefined}).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% priv

set_timer(Delay) ->
    timer:send_after(Delay, self(), {'$gen_cast', refresh}).

recover(Pid, Key) ->
    gen_server:cast(Pid, {recover, recover(Key)}).

recover({DbName, DocId}) ->
    ddoc_cache_opener:recover_doc_info(DbName, DocId).
