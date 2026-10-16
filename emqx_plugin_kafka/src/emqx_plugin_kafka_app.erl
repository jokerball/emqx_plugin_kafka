-module(emqx_plugin_kafka_app).

-behaviour(application).

-emqx_plugin(?MODULE).

%% for logging
-include_lib("emqx_plugin_helper/include/logger.hrl").

-export([
    start/2,
    stop/1
]).

-export([
    on_config_changed/2,
    on_health_check/1
]).

%% NOTE
%% Functions from EMQX are unavailable at compile time.
-dialyzer({no_unknown, [start/2, stop/1]}).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_plugin_kafka_sup:start_link(),
    emqx_plugin_kafka:hook(),
    emqx_ctl:register_command(emqx_plugin_kafka, {emqx_plugin_kafka_cli, cmd}),
    ?SLOG(info, #{msg => "emqx_plugin_kafka_started"}),
    {ok, Sup}.

stop(_State) ->
    emqx_ctl:unregister_command(emqx_plugin_kafka),
    emqx_plugin_kafka:unhook(),
    ?SLOG(debug, #{msg => "emqx_plugin_kafka_stopped"}).

on_config_changed(OldConfig, NewConfig) ->
    emqx_plugin_kafka:on_config_changed(OldConfig, NewConfig).

on_health_check(Options) ->
    emqx_plugin_kafka:on_health_check(Options).
