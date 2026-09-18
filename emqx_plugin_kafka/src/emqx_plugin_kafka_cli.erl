-module(emqx_plugin_kafka_cli).

%% This is an example on how to extend `emqx ctl` with your own commands.

-export([cmd/1]).

%% NOTE
%% Functions from EMQX are unavailable at compile time.
-dialyzer({no_unknown, [cmd/1]}).

cmd(["get-config"]) ->
    Config = emqx_plugin_kafka:get_config(),
    emqx_ctl:print("~s~n", [emqx_utils_json:encode(Config)]);
cmd(["status"]) ->
    case brod_client:get_workers_sup(emqx_repost_worker) of
        {ok, _Pid} ->
            emqx_ctl:print("Kafka client: running~n");
        {error, Reason} ->
            emqx_ctl:print("Kafka client: stopped (~p)~n", [Reason])
    end;
cmd(_) ->
    emqx_ctl:usage([
        {"get-config", "get current plugin config"},
        {"status", "check Kafka client status"}
    ]).
