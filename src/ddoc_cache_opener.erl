% Copyright 2012 Cloudant. All rights reserved.

-module(ddoc_cache_opener).
-behaviour(gen_server).

-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

% worker API
-export([start_link/0]).

% public API
-export([
    open_doc/2,
    open_doc/3,
    open_validation_funs/1,
    evict_docs/2,
    lookup/1,
    match_newest/1,
    recover_doc/2,
    recover_doc/3,
    recover_validation_funs/1
]).

% couch_event listener callback
-export([handle_db_event/3]).

% gen_server behavior
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

% sub-process spawn API
-export([
    fetch_doc_data/1
]).

-define(CACHE, ddoc_cache_lru).
-define(OPENING, ddoc_cache_opening).

-type dbname() :: iodata().
-type docid() :: iodata().
-type revision() :: {pos_integer(), <<_:128>>}.

-record(opener, {
    key,
    pid,
    clients
}).

-record(st, {
    db_ddocs,
    evictor
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec open_doc(dbname(), docid()) -> {ok, #doc{}}.
open_doc(DbName, DocId) ->
    Resp = gen_server:call(?MODULE, {open, {DbName, DocId}}, infinity),
    handle_open_response(Resp).

-spec open_doc(dbname(), docid(), revision()) -> {ok, #doc{}}.
open_doc(DbName, DocId, RevId) ->
    Resp = gen_server:call(?MODULE, {open, {DbName, DocId, RevId}}, infinity),
    handle_open_response(Resp).

-spec open_validation_funs(dbname()) -> {ok, [fun()]}.
open_validation_funs(DbName) ->
    Resp = gen_server:call(?MODULE, {open, {DbName, validation_funs}}, infinity),
    handle_open_response(Resp).

-spec evict_docs(dbname(), [docid()]) -> ok.
evict_docs(DbName, DocIds) ->
    gen_server:cast(?MODULE, {evict, DbName, DocIds}).

lookup(Key) ->
    try ets_lru:lookup_d(?CACHE, Key) of
        {ok, _} = Resp ->
            Resp;
        _ ->
            missing
    catch
        error:badarg ->
            recover
    end.

match_newest(Key) ->
    try ets_lru:match_object(?CACHE, Key, '_') of
        [] ->
            missing;
        Docs ->
            Sorted = lists:sort(fun (#doc{revs=L}, #doc{revs=R}) ->
                L >= R
            end, Docs),
            {ok, hd(Sorted)}
    catch
        error:badarg ->
            recover
    end.

recover_doc(DbName, DDocId) ->
    fabric:open_doc(DbName, DDocId, []).

recover_doc(DbName, DDocId, RevId) ->
    {ok, [Resp]} = fabric:open_revs(DbName, DDocId, [RevId], []),
    Resp.

recover_validation_funs(DbName) ->
    {ok, DDocs} = fabric:design_docs(mem3:dbname(DbName)),
    Funs = lists:flatmap(fun(DDoc) ->
        case couch_doc:get_validate_doc_fun(DDoc) of
            nil -> [];
            Fun -> [Fun]
        end
    end, DDocs),
    {ok, Funs}.

handle_db_event(ShardDbName, created, St) ->
    gen_server:cast(?MODULE, {evict, mem3:dbname(ShardDbName)}),
    {ok, St};
handle_db_event(ShardDbName, deleted, St) ->
    gen_server:cast(?MODULE, {evict, mem3:dbname(ShardDbName)}),
    {ok, St};
handle_db_event(_DbName, _Event, St) ->
    {ok, St}.

init(_) ->
    process_flag(trap_exit, true),
    _ = ets:new(?OPENING, [set, protected, named_table, {keypos, #opener.key}]),
    {ok, Evictor} = couch_event:link_listener(
            ?MODULE, handle_db_event, nil, [all_dbs]
        ),
    {ok, #st{
        evictor = Evictor
    }}.

terminate(_Reason, St) ->
    case is_pid(St#st.evictor) of
        true -> exit(St#st.evictor, kill);
        false -> ok
    end,
    ok.

handle_call({open, OpenerKey}, From, St) ->
    case ets:lookup(?OPENING, OpenerKey) of
        [#opener{clients=Clients}=O] ->
            ets:insert(?OPENING, O#opener{clients=[From | Clients]}),
            {noreply, St};
        [] ->
            Pid = spawn_link(?MODULE, fetch_doc_data, [OpenerKey]),
            ets:insert(?OPENING, #opener{key=OpenerKey, pid=Pid, clients=[From]}),
            {noreply, St}
    end;

handle_call(Msg, _From, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


handle_cast({evict, DbName}, St) ->
    gen_server:abcast(mem3:nodes(), ?MODULE, {do_evict, DbName}),
    {noreply, St};

handle_cast({evict, DbName, DDocIds}, St) ->
    gen_server:abcast(mem3:nodes(), ?MODULE, {do_evict, DbName, DDocIds}),
    {noreply, St};

handle_cast({do_evict, DbName}, St) ->
    DDocIds = lists:flatten(ets_lru:match(?CACHE, {DbName, '$1', '_'}, '_')),
    handle_cast({do_evict, DbName, DDocIds}, St);

handle_cast({do_evict, DbName, DDocIds}, St) ->
    ets_lru:remove(?CACHE, {DbName, validation_funs}),
    lists:foreach(fun(DDocId) ->
        RevIds = ets_lru:match(?CACHE, {DbName, DDocId, '$1'}, '_'),
        lists:foreach(fun([RevId]) ->
            ets_lru:remove(?CACHE, {DbName, DDocId, RevId})
        end, RevIds)
    end, DDocIds),
    {noreply, St};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.

handle_info({'EXIT', Pid, Reason}, #st{evictor=Pid}=St) ->
    twig:log(err, "ddoc_cache_opener evictor died ~w", [Reason]),
    {ok, Evictor} = couch_event:link_listener(?MODULE, handle_db_event, nil, [all_dbs]),
    {noreply, St#st{evictor=Evictor}};

handle_info({'EXIT', _Pid, {open_ok, OpenerKey, Resp}}, St) ->
    respond(OpenerKey, {open_ok, Resp}),
    {noreply, St};

handle_info({'EXIT', _Pid, {open_error, OpenerKey, Type, Error}}, St) ->
    respond(OpenerKey, {open_error, Type, Error}),
    {noreply, St};

handle_info({'EXIT', Pid, Reason}, St) ->
    Pattern = #opener{pid=Pid, _='_'},
    case ets:match_object(?OPENING, Pattern) of
        [#opener{key=OpenerKey, clients=Clients}] ->
            _ = [gen_server:reply(C, {error, Reason}) || C <- Clients],
            ets:delete(?OPENING, OpenerKey),
            {noreply, St};
        [] ->
            {stop, {unknown_pid_died, {Pid, Reason}}, St}
    end;

handle_info(Msg, St) ->
    {stop, {invalid_info, Msg}, St}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec fetch_doc_data({dbname(), validation_funs}) -> no_return();
                    ({dbname(), docid()}) -> no_return();
                    ({dbname(), docid(), revision()}) -> no_return().
fetch_doc_data({DbName, validation_funs}) ->
    OpenerKey = {DbName, validation_funs},
    {ok, Funs} = recover_validation_funs(DbName),
    ok = ets_lru:insert(?CACHE, OpenerKey, Funs),
    exit({open_ok, OpenerKey, {ok, Funs}});
fetch_doc_data({DbName, DocId}) ->
    OpenerKey = {DbName, DocId},
    try recover_doc(DbName, DocId) of
        {ok, Doc} ->
            {RevCount, [RevHash| _]} = Doc#doc.revs,
            RevId = {RevCount, RevHash},
            ok = ets_lru:insert(?CACHE, {DbName, DocId, RevId}, Doc),
            exit({open_ok, OpenerKey, {ok, Doc}});
        Else ->
            exit({open_ok, OpenerKey, Else})
    catch
        Type:Reason ->
            exit({open_error, OpenerKey, Type, Reason})
    end;
fetch_doc_data({DbName, DocId, RevId}) ->
    OpenerKey = {DbName, DocId, RevId},
    try recover_doc(DbName, DocId, RevId) of
        {ok, Doc} ->
            ok = ets_lru:insert(?CACHE, {DbName, DocId, RevId}, Doc),
            exit({open_ok, OpenerKey, {ok, Doc}});
        Else ->
            exit({open_ok, OpenerKey, Else})
    catch
        Type:Reason ->
            exit({open_error, OpenerKey, Type, Reason})
    end.

handle_open_response(Resp) ->
    case Resp of
        {open_ok, Value} -> Value;
        {open_error, throw, Error} -> throw(Error);
        {open_error, error, Error} -> erlang:error(Error);
        {open_error, exit, Error} -> exit(Error)
    end.

respond(OpenerKey, Resp) ->
    [#opener{clients=Clients}] = ets:lookup(?OPENING, OpenerKey),
    _ = [gen_server:reply(C, Resp) || C <- Clients],
    ets:delete(?OPENING, OpenerKey).
