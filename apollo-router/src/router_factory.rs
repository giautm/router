// This entire file is license key functionality
use std::sync::Arc;

use futures::stream::BoxStream;
use serde_json::Map;
use serde_json::Value;
use tower::buffer::Buffer;
use tower::util::BoxCloneService;
use tower::util::BoxService;
use tower::BoxError;
use tower::ServiceBuilder;
use tower::ServiceExt;
use tower_service::Service;

use crate::configuration::Configuration;
use crate::configuration::ConfigurationError;
use crate::graphql;
use crate::http_ext::Request;
use crate::http_ext::Response;
use crate::layers::ServiceBuilderExt;
use crate::plugin::DynPlugin;
use crate::services::Plugins;
use crate::PluggableRouterServiceBuilder;
use crate::Schema;
use crate::SubgraphService;

/// Factory for creating a RouterService
///
/// Instances of this traits are used by the StateMachine to generate a new
/// RouterService from configuration when it changes
#[async_trait::async_trait]
pub(crate) trait RouterServiceFactory: Send + Sync + 'static {
    type RouterService: Service<
            Request<graphql::Request>,
            Response = Response<BoxStream<'static, graphql::Response>>,
            Error = BoxError,
            Future = Self::Future,
        > + Send
        + Sync
        + Clone
        + 'static;
    type Future: Send;

    async fn create<'a>(
        &'a mut self,
        configuration: Arc<Configuration>,
        schema: Arc<crate::Schema>,
        previous_router: Option<&'a Self::RouterService>,
    ) -> Result<(Self::RouterService, Plugins), BoxError>;
}

/// Main implementation of the RouterService factory, supporting the extensions system
#[derive(Default)]
pub(crate) struct YamlRouterServiceFactory;

#[async_trait::async_trait]
impl RouterServiceFactory for YamlRouterServiceFactory {
    type RouterService = Buffer<
        BoxCloneService<
            Request<graphql::Request>,
            Response<BoxStream<'static, graphql::Response>>,
            BoxError,
        >,
        Request<graphql::Request>,
    >;
    type Future = <Self::RouterService as Service<Request<graphql::Request>>>::Future;

    async fn create<'a>(
        &'a mut self,
        configuration: Arc<Configuration>,
        schema: Arc<Schema>,
        _previous_router: Option<&'a Self::RouterService>,
    ) -> Result<(Self::RouterService, Plugins), BoxError> {
        let mut builder = PluggableRouterServiceBuilder::new(schema.clone());
        if configuration.server.introspection {
            builder = builder.with_naive_introspection();
        }

        for (name, _) in schema.subgraphs() {
            let subgraph_service = BoxService::new(SubgraphService::new(name.to_string()));

            builder = builder.with_subgraph_service(name, subgraph_service);
        }
        // Process the plugins.
        let plugins = create_plugins(&configuration, &schema).await?;

        for (plugin_name, plugin) in plugins {
            builder = builder.with_dyn_plugin(plugin_name, plugin);
        }

        let (pluggable_router_service, mut plugins) = builder.build().await?;
        let service = ServiceBuilder::new().buffered().service(
            pluggable_router_service
                .map_request(|http_request: Request<graphql::Request>| http_request.into())
                .map_response(|response| response.response)
                .boxed_clone(),
        );

        // We're good to go with the new service. Let the plugins know that this is about to happen.
        // This is needed so that the Telemetry plugin can swap in the new propagator.
        // The alternative is that we introduce another service on Plugin that wraps the request
        // at a much earlier stage.
        for (_, plugin) in &mut plugins {
            tracing::debug!("activating plugin {}", plugin.name());
            plugin.activate();
            tracing::debug!("activated plugin {}", plugin.name());
        }

        Ok((service, plugins))
    }
}

async fn create_plugins(
    configuration: &Configuration,
    schema: &Schema,
) -> Result<Vec<(String, Box<dyn DynPlugin>)>, BoxError> {
    // List of mandatory plugins. Ordering is important!!
    let mut mandatory_plugins = vec!["experimental.include_subgraph_errors", "apollo.csrf"];

    // Telemetry is *only* mandatory if the global subscriber is set
    if crate::subscriber::is_global_subscriber_set() {
        mandatory_plugins.insert(0, "apollo.telemetry");
    }

    let mut errors = Vec::new();
    let plugin_registry = crate::plugin::plugins();
    let mut plugin_instances = Vec::new();

    for (name, mut configuration) in configuration.plugins().into_iter() {
        let name = name.clone();

        match plugin_registry.get(name.as_str()) {
            Some(factory) => {
                tracing::debug!(
                    "creating plugin: '{}' with configuration:\n{:#}",
                    name,
                    configuration
                );
                if name == "apollo.telemetry" {
                    inject_schema_id(schema, &mut configuration);
                }
                // expand any env variables in the config before processing.
                match factory.create_instance(&configuration).await {
                    Ok(plugin) => {
                        plugin_instances.push((name, plugin));
                    }
                    Err(err) => errors.push(ConfigurationError::PluginConfiguration {
                        plugin: name,
                        error: err.to_string(),
                    }),
                }
            }
            None => errors.push(ConfigurationError::PluginUnknown(name)),
        }
    }

    // At this point we've processed all of the plugins that were provided in configuration.
    // We now need to do process our list of mandatory plugins:
    //  - If a mandatory plugin is already in the list, then it must be re-located
    //    to its mandatory location
    //  - If it is missing, it must be added at its mandatory location

    for (desired_position, name) in mandatory_plugins.iter().enumerate() {
        let position_maybe = plugin_instances.iter().position(|(x, _)| x == name);
        match position_maybe {
            Some(actual_position) => {
                // Found it, re-locate if required.
                if actual_position != desired_position {
                    let temp = plugin_instances.remove(actual_position);
                    plugin_instances.insert(desired_position, temp);
                }
            }
            None => {
                // Didn't find it, insert
                match plugin_registry.get(*name) {
                    // Create an instance
                    Some(factory) => {
                        // Create default (empty) config
                        let mut config = Value::Object(Map::new());
                        // The apollo.telemetry" plugin isn't happy with empty config, so we
                        // give it some. If any of the other mandatory plugins need special
                        // treatment, then we'll have to perform it here.
                        // This is *required* by the telemetry module or it will fail...
                        if *name == "apollo.telemetry" {
                            inject_schema_id(schema, &mut config);
                        }
                        match factory.create_instance(&config).await {
                            Ok(plugin) => {
                                plugin_instances
                                    .insert(desired_position, (name.to_string(), plugin));
                            }
                            Err(err) => errors.push(ConfigurationError::PluginConfiguration {
                                plugin: name.to_string(),
                                error: err.to_string(),
                            }),
                        }
                    }
                    None => errors.push(ConfigurationError::PluginUnknown(name.to_string())),
                }
            }
        }
    }

    let plugin_details = plugin_instances
        .iter()
        .map(|(name, plugin)| (name, plugin.name()))
        .collect::<Vec<(&String, &str)>>();
    tracing::info!(?plugin_details, "list of plugins");

    if !errors.is_empty() {
        for error in &errors {
            tracing::error!("{:#}", error);
        }

        Err(BoxError::from(
            errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join("\n"),
        ))
    } else {
        Ok(plugin_instances)
    }
}

fn inject_schema_id(schema: &Schema, configuration: &mut Value) {
    if configuration.get("apollo").is_none() {
        if let Some(telemetry) = configuration.as_object_mut() {
            telemetry.insert("apollo".to_string(), Value::Object(Default::default()));
        }
    }

    if let (Some(schema_id), Some(apollo)) = (
        &schema.api_schema().schema_id,
        configuration.get_mut("apollo"),
    ) {
        if let Some(apollo) = apollo.as_object_mut() {
            apollo.insert(
                "schema_id".to_string(),
                Value::String(schema_id.to_string()),
            );
        }
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;
    use std::fmt;
    use std::sync::Arc;

    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde_json::json;
    use tower_http::BoxError;

    use crate::configuration::Configuration;
    use crate::plugin::Plugin;
    use crate::register_plugin;
    use crate::router_factory::inject_schema_id;
    use crate::router_factory::RouterServiceFactory;
    use crate::router_factory::YamlRouterServiceFactory;
    use crate::Schema;

    #[derive(Debug)]
    struct PluginError;

    impl fmt::Display for PluginError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "PluginError")
        }
    }

    impl Error for PluginError {}

    // Always starts and stops plugin

    #[derive(Debug)]
    struct AlwaysStartsAndStopsPlugin {}

    #[derive(Debug, Default, Deserialize, JsonSchema)]
    struct Conf {
        name: String,
    }

    #[async_trait::async_trait]
    impl Plugin for AlwaysStartsAndStopsPlugin {
        type Config = Conf;

        async fn new(configuration: Self::Config) -> Result<Self, BoxError> {
            tracing::debug!("{}", configuration.name);
            Ok(AlwaysStartsAndStopsPlugin {})
        }
    }

    register_plugin!(
        "apollo.test",
        "always_starts_and_stops",
        AlwaysStartsAndStopsPlugin
    );

    // Always fails to start plugin

    #[derive(Debug)]
    struct AlwaysFailsToStartPlugin {}

    #[async_trait::async_trait]
    impl Plugin for AlwaysFailsToStartPlugin {
        type Config = Conf;

        async fn new(configuration: Self::Config) -> Result<Self, BoxError> {
            tracing::debug!("{}", configuration.name);
            Err(BoxError::from("Error"))
        }
    }

    register_plugin!(
        "apollo.test",
        "always_fails_to_start",
        AlwaysFailsToStartPlugin
    );

    #[tokio::test]
    async fn test_yaml_no_extras() {
        let config = Configuration::builder().build();
        let service = create_service(config).await;
        assert!(service.is_ok())
    }

    #[tokio::test]
    async fn test_yaml_plugins_always_starts_and_stops() {
        let config: Configuration = serde_yaml::from_str(
            r#"
            plugins:
                apollo.test.always_starts_and_stops:
                    name: albert
        "#,
        )
        .unwrap();
        let service = create_service(config).await;
        assert!(service.is_ok())
    }

    #[tokio::test]
    async fn test_yaml_plugins_always_fails_to_start() {
        let config: Configuration = serde_yaml::from_str(
            r#"
            plugins:
                apollo.test.always_fails_to_start:
                    name: albert
        "#,
        )
        .unwrap();
        let service = create_service(config).await;
        assert!(service.is_err())
    }

    #[tokio::test]
    async fn test_yaml_plugins_combo_start_and_fail() {
        let config: Configuration = serde_yaml::from_str(
            r#"
            plugins:
                apollo.test.always_starts_and_stops:
                    name: albert
                apollo.test.always_fails_to_start:
                    name: albert
        "#,
        )
        .unwrap();
        let service = create_service(config).await;
        assert!(service.is_err())
    }

    // This test must use the multi_thread tokio executor or the opentelemetry hang bug will
    // be encountered. (See https://github.com/open-telemetry/opentelemetry-rust/issues/536)
    #[tokio::test(flavor = "multi_thread")]
    async fn test_telemetry_doesnt_hang_with_invalid_schema() {
        use tracing_subscriber::EnvFilter;

        use crate::subscriber::set_global_subscriber;
        use crate::subscriber::RouterSubscriber;

        // A global subscriber must be set before we start up the telemetry plugin
        let _ = set_global_subscriber(RouterSubscriber::JsonSubscriber(
            tracing_subscriber::fmt::fmt()
                .with_env_filter(EnvFilter::from_default_env())
                .json()
                .finish(),
        ));

        let config: Configuration = serde_yaml::from_str(
            r#"
            telemetry:
              tracing:
                trace_config:
                  service_name: router
                otlp:
                  endpoint: default
        "#,
        )
        .unwrap();

        let schema: Schema = include_str!("testdata/invalid_supergraph.graphql")
            .parse()
            .unwrap();

        let service = YamlRouterServiceFactory::default()
            .create(Arc::new(config), Arc::new(schema), None)
            .await;
        service.map(|_| ()).unwrap_err();
    }

    async fn create_service(config: Configuration) -> Result<(), BoxError> {
        let schema: Schema = include_str!("testdata/supergraph.graphql").parse().unwrap();

        let service = YamlRouterServiceFactory::default()
            .create(Arc::new(config), Arc::new(schema), None)
            .await;
        service.map(|_| ())
    }

    #[test]
    fn test_inject_schema_id() {
        let schema = include_str!("testdata/starstuff@current.graphql")
            .parse()
            .unwrap();
        let mut config = json!({});
        inject_schema_id(&schema, &mut config);
        let config =
            serde_json::from_value::<crate::plugins::telemetry::config::Conf>(config).unwrap();
        assert_eq!(
            &config.apollo.unwrap().schema_id,
            "ba573b479c8b3fa273f439b26b9eda700152341d897f18090d52cd073b15f909"
        );
    }
}
