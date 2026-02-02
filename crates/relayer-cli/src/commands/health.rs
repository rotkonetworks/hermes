use abscissa_core::clap::Parser;

use ibc_relayer::chain::endpoint::HealthCheck::*;
use ibc_relayer::chain::handle::ChainHandle;
use ibc_relayer::config::ChainConfig;

use crate::cli_utils::spawn_chain_runtime;
use crate::conclude::{exit_with_unrecoverable_error, Output};
use crate::prelude::*;

#[derive(Clone, Command, Debug, Parser)]
pub struct HealthCheckCmd {}

impl Runnable for HealthCheckCmd {
    fn run(&self) {
        let config = app_config();

        for ch in &config.chains {
            let _span = tracing::error_span!("health_check", chain = %ch.id()).entered();

            info!("performing health check...");

            let chain =
                spawn_chain_runtime(&config, ch.id()).unwrap_or_else(exit_with_unrecoverable_error);

            match chain.health_check() {
                Ok(Healthy) => info!("chain is healthy"),
                Ok(Unhealthy(_)) => {
                    // No need to print the error here as it's already printed in `Chain::health_check`
                    // TODO(romac): Move the printing code here and in the supervisor/registry
                    warn!("chain is not healthy")
                }
                Err(e) => error!("failed to perform health check, reason: {}", e.detail()),
            }

            // Report client refresh status
            report_client_refresh_status(ch);
        }

        Output::success_msg("performed health check for all chains in the config").exit()
    }
}

fn report_client_refresh_status(chain_config: &ChainConfig) {
    match chain_config {
        ChainConfig::CosmosSdk(config) | ChainConfig::Namada(config) => {
            let refresh_rate = config.client_refresh_rate.as_f64();
            if refresh_rate > 0.0 {
                if let Some(trusting_period) = config.trusting_period {
                    let refresh_interval = trusting_period.as_secs_f64() * refresh_rate;
                    let interval_hours = refresh_interval / 3600.0;
                    if interval_hours >= 24.0 {
                        info!(
                            "client auto-refresh: enabled (every {:.1} days)",
                            interval_hours / 24.0
                        );
                    } else {
                        info!(
                            "client auto-refresh: enabled (every {:.1} hours)",
                            interval_hours
                        );
                    }
                } else {
                    info!(
                        "client auto-refresh: enabled (rate {:.2} of trusting period)",
                        refresh_rate
                    );
                }
            } else {
                info!("client auto-refresh: disabled");
            }
        }
        ChainConfig::Penumbra(_) => {
            // Penumbra doesn't use standard client refresh
            info!("client auto-refresh: n/a (penumbra)");
        }
    }
}
