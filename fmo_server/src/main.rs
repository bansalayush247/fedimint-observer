use fmo_core::FedimintObserver;
use fmo_module_ln::LnObserver;
use fmo_module_mint::MintObserver;
use fmo_module_wallet::WalletObserver;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    FedimintObserver::builder()
        .with_module(MintObserver)
        .with_module(WalletObserver)
        .with_module(LnObserver)
        .run()
        .await
}
