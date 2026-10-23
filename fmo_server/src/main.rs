use fmo_core::FedimintObserver;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    FedimintObserver::builder().run().await
}
