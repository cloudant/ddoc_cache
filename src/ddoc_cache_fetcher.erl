% Copyright 2014 Cloudant. All rights reserved.

-module(ddoc_cache_fetcher).

-behaviour(gen_server).
-vsn(1).

-export([start_link/1, start/1, open/1, open/2]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2,
    handle_info/2, terminate/2, code_change/3]).

-include("ddoc_cache.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(CACHE, ddoc_cache_lru).
-record(state, {key, pid, doc, rev, queue = []}).


-spec start_link(doc_key()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% @doc - Starts fetcher with a given key under supervision tree
-spec start(doc_key()) -> {ok, pid()} | {ok, 'ignore'} | {error, term()}.
start(Key) ->
    ChildSpec = {{fetcher, Key}, ?MODULE, [Key]},
    ddoc_cache_fetcher_sup:start_child(ChildSpec).

-spec open(doc_key()) -> {ok, #doc{}} | {error, term()}.
open(Key) ->
    {ok, Pid} = start(Key),
    case erlang:is_process_alive(Pid) of
    true ->
        gen_server:call(Pid, open);
    false ->
        ddoc_cache_opener:lookup(Key)
    end.

-spec open(doc_key(), term()) -> {ok, #doc{}}.
open(Key, To) ->
    {ok, Pid} = start(Key),
    case erlang:is_process_alive(Pid) of
    true ->
        gen_server:cast(Pid, {open, To});
    false ->
        Reply = ddoc_cache_opener:lookup(Key),
        gen_server:reply(To, Reply)
    end.


init(Key) ->
    process_flag(trap_exit, true),
    {ok, #state{key = Key}, 0}.

handle_call(open, From, #state{queue = Q} = State) ->
    {noreply, State#state{queue = [From|Q]}};
handle_call(Msg, _, State) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, State}.

handle_cast({open, To}, #state{queue = Q} = State) ->
    {noreply, State#state{queue = [To|Q]}};
handle_cast({recover, {ok, Doc}}, #state{key = Key} = State) ->
    ok = store_ddoc(Key, Doc),
    maybe_start_synchronizer(Key, Doc),
    {noreply, State#state{doc = {ok, Doc}}};
handle_cast({recover, Reply}, State) ->
    {noreply, State#state{doc = Reply}};
handle_cast(reply, #state{doc = Reply, queue = Q} = State) ->
    [gen_server:reply(From, Reply) || From <- Q],
    {stop, normal, State#state{queue = []}};
handle_cast(Msg, State) ->
    {stop, {invalid_cast, Msg}, State}.

handle_info(timeout, #state{key = Key} = State) ->
    maybe_stop_synchronizer(Key),
    Self = self(),
    Pid = proc_lib:spawn_link(fun() ->
        erlang:apply(fun recover/2, [Self, Key])
    end),
    {noreply, State#state{pid = Pid}};
handle_info({'EXIT', Pid, normal}, #state{pid=Pid} = State) ->
    gen_server:cast(self(), reply),
    {noreply, State#state{pid = undefined}};
handle_info({'EXIT', Pid, Reason}, #state{pid=Pid} = State) ->
    {stop, Reason, State#state{pid = undefined}};
handle_info(Msg, State) ->
    {stop, {invalid_info, Msg}, State}.

terminate(Reason, #state{pid = Pid}) ->
    case is_pid(Pid) andalso erlang:is_process_alive(Pid) of
    true -> exit(Pid, Reason);
    false -> ok
    end.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% priv

recover(Pid, Key) ->
    gen_server:cast(Pid, {recover, recover(Key)}).

recover({DbName, custom, validation_funs}) ->
    ddoc_cache_opener:recover_validation_funs(DbName);
recover({DbName, custom, Mod}) when is_atom(Mod) ->
    Mod:recover(DbName);
recover({DbName, DocId}) ->
    ddoc_cache_opener:recover_doc(DbName, DocId);
recover({DbName, DocId, Rev}) ->
    ddoc_cache_opener:recover_doc(DbName, DocId, Rev).

store_ddoc(Key, Doc) ->
    ok = ets_lru:insert(?CACHE, Key, Doc).

maybe_start_synchronizer({DbName, DocId}, Doc) when not is_atom(DocId) ->
    {RevDepth, [RevHash| _]} = Doc#doc.revs,
    Rev = {RevDepth, RevHash},
    ok = store_ddoc({DbName, DocId, Rev}, Doc),
    {ok, _} = ddoc_cache_synchronizer:start({DbName, DocId}, Rev);
maybe_start_synchronizer({_, custom, _}, _) ->
    ok;
maybe_start_synchronizer({DbName, DocId, Rev}, Doc) ->
    case ddoc_cache_opener:member({DbName, DocId}) of
    true -> ok;
    false ->
        ok = ddoc_cache_opener:store_doc({DbName, DocId}, Doc),
        {ok, _} = ddoc_cache_synchronizer:start({DbName, DocId}, Rev)
    end.

maybe_stop_synchronizer({DbName, DocId}) ->
    ddoc_cache_synchronizer:stop({DbName, DocId});
maybe_stop_synchronizer(_) ->
    ok.
