#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmo_core::run().await
}
