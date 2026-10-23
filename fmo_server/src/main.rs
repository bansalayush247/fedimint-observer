use fmo_core::FedimintObserver;
use fmo_module_mint::MintObserver;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    FedimintObserver::builder()
        .with_module(MintObserver)
        .run()
        .await
}
