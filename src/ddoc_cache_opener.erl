% Copyright 2012 Cloudant. All rights reserved.

-module(ddoc_cache_opener).
-behaviour(gen_server).
-vsn(2).

-include("ddoc_cache.hrl").
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
    open_custom/2,
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

-record(st, {
    limiter,
    evictor
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec open_doc(db_name(), doc_id()) ->
    {ok, #doc{}} | {error, term()}.
open_doc(DbName, DocId) ->
    ddoc_cache_fetcher:open({DbName, DocId}).

-spec open_doc(db_name(), doc_id(), revision()) ->
    {ok, #doc{}} | {error, term()}.
open_doc(DbName, DocId, Rev) ->
    ddoc_cache_fetcher:open({DbName, DocId, Rev}).

-spec open_validation_funs(db_name()) ->
    {ok, [fun()]} | {error, term()}.
open_validation_funs(DbName) ->
    ddoc_cache_fetcher:open({DbName, custom, validation_funs}).

-spec open_custom(db_name(), atom()) ->
    {ok, [term()]} | {error, term()}.
open_custom(DbName, Mod) ->
    ddoc_cache_fetcher:open({DbName, custom, Mod}).

-spec evict_docs(db_name(), [doc_id()]) -> ok.
evict_docs(DbName, DocIds) ->
    gen_server:cast(?MODULE, {evict, DbName, DocIds}).

-spec lookup(doc_key()) -> {ok, #doc{}} | missing | recover.
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

-spec match_newest(doc_key()) -> {ok, #doc{}} | missing | recover.
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
-spec recover_doc(db_name(), doc_id()) ->
    {ok, #doc{}} |
    {not_found, missing | deleted} |
    {timeout, any()} |
    {error, any()} |
    {error, any(), any()}.
recover_doc(DbName, DDocId) ->
    fabric:open_doc(DbName, DDocId, []).

%% @doc Returns the given revision of design doc
-spec recover_doc(db_name(), doc_id(), revision()) ->
    {ok, #doc{}} |
    {not_found, missing | deleted} |
    {timeout, any()} |
    {error, any()} |
    {error, any(), any()}.
recover_doc(DbName, DDocId, Rev) ->
    {ok, [Resp]} = fabric:open_revs(DbName, DDocId, [Rev], []),
    Resp.

%% @doc Retrieves an information on a document with a given id
-spec recover_doc_info(db_name(), doc_id()) ->
    {ok, #doc_info{}} |
    {not_found, missing} |
    {timeout, any()} |
    {error, any()} |
    {error, any(), any()}.
recover_doc_info(DbName, DDocId) ->
    fabric:get_doc_info(DbName, DDocId, [{r, "1"}]).

%% @doc Returns a list of all the validation funs of the design docs
%% in a given database
-spec recover_validation_funs(db_name()) -> {ok, [fun()]}.
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
    ets:new(?CACHE, [set, named_table, public, {keypos, #entry.key}]),
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
    ddoc_cache_fetcher:open(OpenerKey, From),
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
        ddoc_cache_synchronizer:stop({DbName, Mod}),
        ets_lru:remove(?CACHE, {DbName, Mod})
    end, CustomKeys),
    lists:foreach(fun(DDocId) ->
        ddoc_cache_fetcher:start({DbName, DDocId})
    end, DDocIds),
    {noreply, St};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.

handle_info({'EXIT', Pid, Reason}, #st{evictor=Pid}=St) ->
    twig:log(err, "ddoc_cache_opener evictor died ~w", [Reason]),
    {ok, Evictor} = couch_event:link_listener(
        ?MODULE, handle_db_event, nil, [all_dbs]
    ),
    {noreply, St#st{evictor=Evictor}};

handle_info(Msg, St) ->
    {stop, {invalid_info, Msg}, St}.

code_change(1, #st{evictor=Evictor}, _Extra) ->
    {ok, #st{evictor = Evictor, limiter = get_limiter()}};
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


trim_cache(Limiter) ->
    Pattern = #entry{key={'$2', '$3', {'$4', '$5'}}, ts='$1', _='_'},
    MapFold = fun([Ts,DbName,DDocId,RevNum,RevHash], Dict) ->
        Key = {DbName, DDocId, {RevNum, RevHash}},
        case dict:find({DbName, DDocId}, Dict) of
        {ok, {N,_} = MaxRev} when N > RevNum ->
            MaxKey = {DbName, DDocId, MaxRev},
            {{Ts, Key, MaxKey}, Dict};
        _ ->
            MaxRev = {RevNum, RevHash},
            {{Ts, Key, Key}, dict:store({DbName, DDocId}, MaxRev, Dict)}
        end
    end,
    {RevKeys, _} = lists:mapfoldl(MapFold, dict:new(),
        ets:match(?CACHE, Pattern)),
    trim_cache(lists:sort(RevKeys), true, Limiter).

trim_cache(_, false, _) ->
    ok;
trim_cache([{_, Key, Key}|T], true, Limiter) ->
    {DbName, DDocId, _} = Key,
    remove_doc(Key),
    remove_doc({DbName, DDocId}),
    remove_match_docs({DbName, custom, '_'}),
    trim_cache(T, Limiter(), Limiter);
trim_cache([{_, Key, _}|T], true, Limiter) ->
    remove_doc(Key),
    trim_cache(T, Limiter(), Limiter);
trim_cache(_,_,_) ->
    ok.

timestamp() ->
    {Mg,S,M} = os:timestamp(),
    Mg * 1000000 * 1000000 + S * 1000000 + M.

get_limiter() ->
    MaxObj = config:get_integer("ddoc_cache", "max_objects", 0),
    SizePredicate = fun() ->
        MaxObj > 0 andalso ets:info(?CACHE, size) > MaxObj
    end,
    MaxMem = config:get_integer("ddoc_cache", "max_size", 104857600),
    MemPredicate = fun() ->
        MaxMem > 0 andalso ets:info(?CACHE, memory) > MaxMem
    end,
    fun() ->
        SizePredicate() or MemPredicate()
    end.
