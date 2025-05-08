use goose::{
    goose::{Scenario, Transaction, TransactionFunction},
    scenario, GooseAttack, GooseError,
};
use lard_egress::StationsResp;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    let mut scenario = scenario!("EgressUser");

    // TODO: figure out how to put the real egress IP in there
    let stations: StationsResp = reqwest::get("https://EGRESS_IP/stations")
        .await?
        .json()
        .await?;

    for (station_id, params) in stations.stations {
        for param_id in params {
            let closure: TransactionFunction = Arc::new(move |user| {
                Box::pin(async move {
                    let path = format!("/stations/{station_id}/params/{param_id}");
                    let _goose = user.get(path.as_str()).await?;

                    Ok(())
                })
            });

            let transaction = Transaction::new(closure);

            let new_scenario = scenario.register_transaction(transaction);
            scenario = new_scenario;
        }
    }

    GooseAttack::initialize()?
        .set_scheduler(goose::GooseScheduler::Random)
        .register_scenario(scenario)
        .execute()
        .await?;

    Ok(())
}
