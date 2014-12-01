% Copyright 2014 Cloudant. All rights reserved.

-module(ddoc_cache_fetcher).

-behaviour(gen_server).

-export([start_link/1, open/1, open/2, recover/2, recover_info/2, refresh/1]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2,
    handle_info/2, terminate/2, code_change/3]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

-define(CACHE, ddoc_cache_lru).
-define(REFRESH, 60000).
-record(state, {key, rev, handler = recover, pid, tref, queue = []}).

-type db_name() :: iodata().
-type doc_id() :: iodata().
-type doc_hash() :: <<_:128>>.
-type revision() :: {pos_integer(), doc_hash()}.
-type doc_key() :: {db_name(), doc_id()} | {db_name(), atom()}
    | {db_name(), doc_id(), revision()}.

-spec start_link(doc_key()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).

-spec open(pid()) -> {ok, #doc{}}.
open(Pid) ->
    gen_server:call(Pid, open).

-spec open(pid(), term()) -> {ok, #doc{}}.
open(Pid, To) ->
    gen_server:call(Pid, {open, To}).

-spec refresh(pid()) -> ok.
refresh(Pid) ->
    gen_server:cast(Pid, refresh).

-spec recover(pid(), doc_key()) -> {ok, #doc{}} | {error, term()}.
recover(Pid, Key) ->
    gen_server:cast(Pid, {recover, recover(Key)}).

-spec recover_info(pid(), doc_key()) -> {ok, #doc_info{}} | {error, term()}.
recover_info(Pid, Key) ->
    gen_server:cast(Pid, {recover_info, recover_info(Key)}).


init(Key) ->
    process_flag(trap_exit, true),
    {ok, #state{key = Key}, 0}.

handle_call(open, From, #state{queue = Q, tref = undefined} = State) ->
    {noreply, State#state{queue = [From|Q]}};
handle_call(open, From, #state{queue = Q, tref = TRef} = State) ->
    {ok, cancel} = timer:cancel(TRef),
    {noreply, State#state{queue=[From|Q], tref=undefined, handler=recover}, 0};
handle_call({open, To}, _From, #state{queue = Q} = State) ->
    {noreply, State#state{queue = [To|Q]}}.

handle_cast(refresh, #state{tref = undefined} = State) ->
    {noreply, State};
handle_cast(refresh, #state{key = {DbName, DocId}, rev = Rev} = State) ->
    {ok, cancel} = timer:cancel(State#state.tref),
    case ets_lru:member(?CACHE, {DbName, DocId, Rev}) of
    true ->
        {noreply, State#state{tref = undefined, handler = recover_info}, 0};
    false ->
        {stop, normal, State#state{tref = undefined}}
    end;
handle_cast({recover_info, {ok, #doc_info{revs = RecRevs}}}, State) ->
    Revs = [R || #rev_info{rev=R, deleted=D} <- RecRevs, D /= true],
    [NewRev|_] = lists:sort(fun({A,_}, {B,_}) -> A > B end, Revs),
    case NewRev =:= State#state.rev of
    true ->
        {noreply, State};
    false ->
        {noreply, State#state{pid=undefined, handler=recover}, 0}
    end;
handle_cast({recover_info, Reply}, #state{key = Key} = State) ->
    twig:log(notice, "can't get doc info for ~p : ~w", [Key, Reply]),
    {noreply, State};
handle_cast({recover, {ok, Doc}}, #state{key = Key, queue = Q} = State) ->
    case Key of
    {_, DocId} when not is_atom(DocId) ->
        {RevDepth, [RevHash| _]} = Doc#doc.revs,
        Rev = {RevDepth, RevHash},
        ok = store_ddoc(Key, Doc, Rev),
        [gen_server:reply(From, {ok, Doc}) || From <- Q],
        {noreply, State#state{queue = [], rev = Rev}};
    _ ->
        ok = store_ddoc(Key, Doc),
        [gen_server:reply(From, {ok, Doc}) || From <- Q],
        {noreply, State#state{queue = []}}
    end;
handle_cast({recover, Reply}, #state{queue = Q} = State) ->
    [gen_server:reply(From, Reply) || From <- Q],
    {noreply, State#state{queue = []}}.

handle_info(timeout, #state{key = Key, handler = Handler} = State) ->
    Self = self(),
    Pid = proc_lib:spawn_link(fun() ->
        erlang:apply(?MODULE, Handler, [Self, Key])
    end),
    {noreply, State#state{pid = Pid}};
handle_info({'EXIT', Pid, normal}, #state{pid=Pid, rev=undefined} = State) ->
    {stop, normal, State#state{pid = undefined}};
handle_info({'EXIT', Pid, normal}, #state{pid=Pid} = State) ->
    {ok, TRef} = timer:send_after(?REFRESH, self(), {'$gen_cast', refresh}),
    {noreply, State#state{pid = undefined, tref = TRef}, hibernate};
handle_info({'EXIT', Pid, Err}, #state{pid=Pid} = State) ->
    {stop, {error, Err}, State#state{pid = undefined}};
handle_info({'EXIT', _, _Reason}, State) ->
    {noreply, State};
handle_info(Msg, State) ->
    {stop, {unknown_msg, Msg}, State}.

terminate(Reason, #state{pid = Pid}) ->
    case is_pid(Pid) andalso erlang:is_process_alive(Pid) of
    true -> exit(Pid, Reason);
    false -> ok
    end.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% priv

recover({DbName, validation_funs}) ->
    ddoc_cache_opener:recover_validation_funs(DbName);
recover({DbName, Mod}) when is_atom(Mod) ->
    Mod:recover(DbName);
recover({DbName, DocId}) ->
    ddoc_cache_opener:recover_doc(DbName, DocId);
recover({DbName, DocId, Rev}) ->
    ddoc_cache_opener:recover_doc(DbName, DocId, Rev).

recover_info({DbName, DocId}) ->
    ddoc_cache_opener:recover_doc_info(DbName, DocId).

store_ddoc(Key, Doc) ->
    ok = ets_lru:insert(?CACHE, Key, Doc).

store_ddoc({DbName, DocId} = Key, Doc, Rev) ->
    ok = ets_lru:insert(?CACHE, {DbName, DocId, Rev}, Doc),
    ok = ddoc_cache_fetcher_sup:set_revision(Key, Rev).