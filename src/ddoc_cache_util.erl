-module(ddoc_cache_util).


-include("ddoc_cache.hrl").


-export([
    open/1,
    new_uuid/0
]).


%% @doc Returns the latest version of design doc
-spec open(db_name(), doc_id()) ->
    {ok, any()} |
    {not_found, missing | deleted} |
    {timeout, any()} |
    {error, any()} |
    {error, any(), any()}.

open({DbName, custom, validation_funs}) ->
    {ok, DDocs} = fabric:design_docs(mem3:dbname(DbName)),
    Funs = lists:flatmap(fun(DDoc) ->
        case couch_doc:get_validate_doc_fun(DDoc) of
            nil -> [];
            Fun -> [Fun]
        end
    end, DDocs),
    {ok, Funs};

open({DbName, custom, Mod}) when is_atom(Mod) ->
    Mod:recover(DbName);

open({DbName, DocId}) ->
    fabric:open_doc(DbName, DDocId, []);

open({DbName, DocId, Rev}) ->
    {ok, [Resp]} = fabric:open_revs(DbName, DDocId, [Rev], []),
    Resp.



% Generate a hexadecimal encoded UUID
new_uuid() ->
    to_hex(crypto:rand_bytes(16), []).


to_hex(<<>>, Acc) ->
    list_to_binary(lists:reverse(Acc));
to_hex(<<C1:4, C2:4, Rest/binary>>, Acc) ->
    to_hex(Rest, [hexdig(C1), hexdig(C2) | Acc]).


hexdig(C) when C >= 0, C =< 9 ->
    C + $0;
hexdig(C) when C >= 10, C =< 15 ->
    C + $A - 10.
