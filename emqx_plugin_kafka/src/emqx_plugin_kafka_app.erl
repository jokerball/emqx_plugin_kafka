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
    logger:warning("=== emqx_plugin_kafka_app:start called ===~n"),
    ?SLOG(warning, #{msg => "emqx_plugin_kafka_app_starting"}),
    {ok, Sup} = emqx_plugin_kafka_sup:start_link(),
    logger:warning("=== emqx_plugin_kafka sup started ===~n"),
    ?SLOG(warning, #{msg => "emqx_plugin_kafka_sup_started"}),
    emqx_plugin_kafka:hook(),
    logger:warning("=== emqx_plugin_kafka hooks registered ===~n"),
    ?SLOG(warning, #{msg => "emqx_plugin_kafka_hooks_registered"}),
    emqx_ctl:register_command(emqx_plugin_kafka, {emqx_plugin_kafka_cli, cmd}),
    logger:warning("=== emqx_plugin_kafka fully started ===~n"),
    ?SLOG(warning, #{msg => "emqx_plugin_kafka_fully_started"}),
    {ok, Sup}.

stop(_State) ->
    ?SLOG(warning, #{msg => "emqx_plugin_kafka_stopping"}),
    emqx_ctl:unregister_command(emqx_plugin_kafka),
    emqx_plugin_kafka:unhook().

on_config_changed(OldConfig, NewConfig) ->
    emqx_plugin_kafka:on_config_changed(OldConfig, NewConfig).

on_health_check(Options) ->
    emqx_plugin_kafka:on_health_check(Options).
