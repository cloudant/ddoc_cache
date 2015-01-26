% Copyright 2012 Cloudant. All rights reserved.

-module(ddoc_cache_opener).
-behaviour(gen_server).
-vsn(2).

-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

-export([
    start_link/0
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
    open_doc/2,
    open_doc/3,
    open_validation_funs/1,
    evict_docs/2,
    lookup/1,
    match_newest/1,
    recover_doc/2,
    recover_doc/3,
    recover_doc_info/2,
    recover_validation_funs/1
]).
-export([
    handle_db_event/3
]).

-define(CACHE, ddoc_cache_lru).

-type dbname() :: iodata().
-type docid() :: iodata() | atom().
-type doc_hash() :: <<_:128>>.
-type revision() :: {pos_integer(), doc_hash()}.

-record(st, {
    db_ddocs,
    evictor
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec open_doc(dbname(), docid()) ->
    {ok, #doc{}} | {error, term()}.
open_doc(DbName, DocId) ->
    open_doc({DbName, DocId}).

-spec open_doc(dbname(), docid(), revision()) ->
    {ok, #doc{}} | {error, term()}.
open_doc(DbName, DocId, Rev) ->
    open_doc({DbName, DocId, Rev}).

-spec open_validation_funs(dbname()) ->
    {ok, [fun()]} | {error, term()}.
open_validation_funs(DbName) ->
    open_doc({DbName, validation_funs}).

open_doc(Key) ->
    {ok, Pid} = ddoc_cache_fetcher_sup:start_child(Key),
    ddoc_cache_fetcher:open(Pid).

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
            Sorted = lists:sort(
                fun (#doc{deleted=DelL, revs=L}, #doc{deleted=DelR, revs=R}) ->
                    {not DelL, L} > {not DelR, R}
                end, Docs),
            {ok, hd(Sorted)}
    catch
        error:badarg ->
            recover
    end.

%% @doc Returns the latest version of design doc
-spec recover_doc(dbname(), docid()) ->
    {ok, #doc{}} |
    {not_found, missing | deleted} |
    {timeout, any()} |
    {error, any()} |
    {error, any() | any()}.
recover_doc(DbName, DDocId) ->
    fabric:open_doc(DbName, DDocId, []).

%% @doc Returns the given revision of design doc
-spec recover_doc(dbname(), docid(), revision()) ->
    {ok, #doc{}} |
    {not_found, missing | deleted} |
    {timeout, any()} |
    {error, any()} |
    {error, any() | any()}.
recover_doc(DbName, DDocId, Rev) ->
    {ok, [Resp]} = fabric:open_revs(DbName, DDocId, [Rev], []),
    Resp.

%% @doc Retrieves an information on a document with a given id
-spec recover_doc_info(dbname(), docid()) ->
    {ok, #doc_info{}} |
    {not_found, missing} |
    {timeout, any()} |
    {error, any()} |
    {error, any() | any()}.
recover_doc_info(DbName, DDocId) ->
    fabric:get_doc_info(DbName, DDocId, [{r, "1"}]).

%% @doc Returns a list of all the validation funs of the design docs
%% in a given database
-spec recover_validation_funs(dbname()) -> {ok, [fun()]}.
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
    {ok, Evictor} = couch_event:link_listener(
        ?MODULE, handle_db_event, nil, [all_dbs]
    ),
    {ok, #st{evictor = Evictor}}.

terminate(_Reason, St) ->
    case is_pid(St#st.evictor) of
        true -> exit(St#st.evictor, kill);
        false -> ok
    end,
    ok.

handle_call({open, OpenerKey}, From, St) ->
    {ok, Pid} = ddoc_cache_fetcher_sup:start_child(OpenerKey),
    ddoc_cache_fetcher:open(Pid, From),
    {noreply, St};

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
    CustomKeys = lists:flatten(ets_lru:match(?CACHE, {DbName, '$1'}, '_')),
    lists:foreach(fun(Mod) ->
        ets_lru:remove(?CACHE, {DbName, Mod})
    end, CustomKeys),
    lists:foreach(fun(DDocId) ->
        case ddoc_cache_fetcher_sup:get_child({DbName, DDocId}) of
        {ok, Pid} ->
            ddoc_cache_fetcher:refresh(Pid);
        _ ->
            ok
        end
    end, DDocIds),
    {noreply, St};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.

handle_info({'EXIT', _Pid, {open_ok, _, _}}, St) ->
    {noreply, St};

handle_info({'EXIT', _Pid, {open_error, _, _, _}}, St) ->
    {noreply, St};

handle_info({'EXIT', Pid, Reason}, #st{evictor=Pid}=St) ->
    twig:log(err, "ddoc_cache_opener evictor died ~w", [Reason]),
    {ok, Evictor} = couch_event:link_listener(
        ?MODULE, handle_db_event, nil, [all_dbs]
    ),
    {noreply, St#st{evictor=Evictor}};

handle_info(Msg, St) ->
    {stop, {invalid_info, Msg}, St}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
