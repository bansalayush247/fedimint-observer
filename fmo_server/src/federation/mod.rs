pub mod db;
pub(crate) mod gateways;
mod guardians;
mod meta;
pub(crate) mod nostr;
pub mod observer;
mod session;
mod transaction;

use std::collections::BTreeMap;

use anyhow::Context;
use axum::extract::{Path, State};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use axum_auth::AuthBearer;
use bitcoin::OutPoint;
use fedimint_core::config::{ClientConfig, FederationId, JsonClientConfig};
use fedimint_core::core::ModuleInstanceId;
use fedimint_core::invite_code::InviteCode;
use fedimint_core::module::registry::ModuleDecoderRegistry;
use fedimint_core::Amount;
use fmo_api_types::{
    FederationSummary, FederationUtxo, FederationUtxosResponse, FedimintTotals,
    GuardianClaimedUtxoState, GuardianUtxoClaim, GuardianUtxoClaimStatus, GuardianUtxoDisagreement,
    GuardianUtxoDisagreementKind, NonceSpendInfo, NoncesRequest,
};
use serde::Deserialize;
use serde_json::json;

use crate::federation::gateways::{get_federation_gateway_uptime_trend, get_federation_gateways};
use crate::federation::guardians::get_federation_health;
use crate::federation::meta::get_federation_meta;
use crate::federation::session::{count_sessions, list_sessions};
use crate::federation::transaction::{
    count_transactions, list_transactions, transaction, transaction_histogram,
};
use crate::util::{config_to_json, get_decoders};
use crate::{federation, AppState};

pub fn get_federations_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(list_observed_federations))
        .route("/", put(add_observed_federation))
        .route("/totals", get(get_federation_totals))
        // TODO: move to nostr module
        .route("/nostr/rating", put(publish_rating_event))
        .route("/:federation_id", get(get_federation_overview))
        .route(
            "/:federation_id/config",
            get(federation::get_federation_config),
        )
        .route("/:federation_id/meta", get(get_federation_meta))
        .route("/:federation_id/health", get(get_federation_health))
        .route("/:federation_id/transactions", get(list_transactions))
        .route(
            "/:federation_id/transactions/:transaction_id",
            get(transaction),
        )
        .route(
            "/:federation_id/transactions/count",
            get(count_transactions),
        )
        .route(
            "/:federation_id/transactions/histogram",
            get(transaction_histogram),
        )
        .route("/:federation_id/gateways", get(get_federation_gateways))
        .route(
            "/:federation_id/gateways/uptime-trend",
            get(get_federation_gateway_uptime_trend),
        )
        .route("/:federation_id/utxos", get(get_federation_utxos))
        .route("/:federation_id/sessions", get(list_sessions))
        .route("/:federation_id/sessions/count", get(count_sessions))
        .route("/:federation_id/backfill", post(backfill_federation))
        .route("/:federation_id/nonces/spend", post(get_nonces_spend_info))
}

pub async fn list_observed_federations(
    State(state): State<AppState>,
) -> crate::error::Result<Json<Vec<FederationSummary>>> {
    Ok(state
        .federation_observer
        .list_federation_summaries()
        .await?
        .into())
}

pub async fn add_observed_federation(
    AuthBearer(auth): AuthBearer,
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> crate::error::Result<Json<FederationId>> {
    state.federation_observer.check_auth(&auth)?;

    let invite: InviteCode = serde_json::from_value(
        body.get("invite")
            .context("Request did not contain invite field")?
            .clone(),
    )
    .context("Invalid invite code")?;
    Ok(state
        .federation_observer
        .add_federation(&invite)
        .await?
        .into())
}

pub(crate) async fn get_federation_config(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<JsonClientConfig>> {
    Ok(config_to_json(
        state
            .federation_observer
            .get_federation(federation_id)
            .await?
            .context("Federation not observed, you might want to try /config/:federation_invite")?
            .config,
    )?
    .into())
}

async fn get_federation_overview(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<serde_json::Value>> {
    let session_count = state
        .federation_observer
        .federation_session_count(federation_id)
        .await?;
    let total_assets_msat = state
        .federation_observer
        .get_federation_assets(federation_id)
        .await?;

    Ok(json!({
        "session_count": session_count,
        "total_assets_msat": total_assets_msat
    })
    .into())
}

async fn get_federation_utxos(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<FederationUtxosResponse>> {
    let observer = &state.federation_observer;
    let (observed, guardian_claims) = tokio::try_join!(
        observer.federation_utxos(federation_id),
        observer.guardian_utxo_claims(federation_id),
    )?;
    let disagreements = guardian_utxo_disagreements(&observed, &guardian_claims);

    Ok(FederationUtxosResponse {
        observed,
        guardian_claims,
        disagreements,
    }
    .into())
}

/// Whether the observer's reconstruction should also hold an output that a
/// guardian reports in this state. The observer records peg-out change once
/// the transaction reaches threshold signatures, while guardians keep it in
/// `UnconfirmedChange` until the finality delay has passed.
fn observer_should_hold(state: GuardianClaimedUtxoState) -> bool {
    matches!(
        state,
        GuardianClaimedUtxoState::Spendable | GuardianClaimedUtxoState::UnconfirmedChange
    )
}

/// Compares the observer's reconstructed UTXO set with every responding
/// guardian's wallet summary, reporting at most one disagreement per output.
fn guardian_utxo_disagreements(
    observed: &[FederationUtxo],
    guardian_claims: &[GuardianUtxoClaim],
) -> Vec<GuardianUtxoDisagreement> {
    let responding = guardian_claims
        .iter()
        .filter(|claim| matches!(claim.status, GuardianUtxoClaimStatus::Ok))
        .collect::<Vec<_>>();
    if responding.is_empty() {
        return Vec::new();
    }

    // Ordered so the disagreement list is stable between requests
    let mut outputs = BTreeMap::<OutPoint, OutputReports>::new();
    for utxo in observed {
        outputs.entry(utxo.out_point).or_default().observed = Some(utxo.amount);
    }
    for claim in &responding {
        for utxo in &claim.utxos {
            if observer_should_hold(utxo.state) {
                let reports = outputs.entry(utxo.out_point).or_default();
                reports.held.push((claim.guardian_id, utxo.amount));
            } else if utxo.state == GuardianClaimedUtxoState::UnsignedChange {
                let reports = outputs.entry(utxo.out_point).or_default();
                reports.unsigned.push(claim.guardian_id);
            }
        }
    }

    outputs
        .into_iter()
        .filter_map(|(out_point, reports)| {
            let (kind, description) = classify_output(&reports, &responding)?;
            Some(GuardianUtxoDisagreement {
                kind,
                out_point,
                description,
            })
        })
        .collect()
}

#[derive(Default)]
struct OutputReports {
    observed: Option<Amount>,
    /// Guardians listing the output in a state the observer should also hold
    held: Vec<(u16, Amount)>,
    /// Guardians that still see the output as change of an unsigned peg-out.
    /// They are a step behind, so they neither confirm nor dispute it.
    unsigned: Vec<u16>,
}

fn classify_output(
    reports: &OutputReports,
    responding: &[&GuardianUtxoClaim],
) -> Option<(GuardianUtxoDisagreementKind, String)> {
    let OutputReports {
        observed,
        held,
        unsigned,
    } = reports;
    let reference = observed.or_else(|| held.first().map(|(_, amount)| *amount))?;
    if held.iter().any(|(_, amount)| *amount != reference) {
        let amounts = observed
            .map(|amount| format!("observer reports {} msat", amount.msats))
            .into_iter()
            .chain(held.iter().map(|(guardian_id, amount)| {
                format!("guardian {guardian_id} reports {} msat", amount.msats)
            }))
            .collect::<Vec<_>>();
        return Some((
            GuardianUtxoDisagreementKind::EvidenceMismatch,
            format!("Reported amounts differ: {}", amounts.join(", ")),
        ));
    }

    let missing = responding
        .iter()
        .map(|claim| claim.guardian_id)
        .filter(|guardian_id| {
            !held.iter().any(|(holder, _)| holder == guardian_id) && !unsigned.contains(guardian_id)
        })
        .collect::<Vec<_>>();

    match (observed.is_some(), held.is_empty(), missing.is_empty()) {
        (_, false, false) => Some((
            GuardianUtxoDisagreementKind::InventoryDifference,
            format!(
                "Guardians disagree: reported by guardians {}, missing from guardians {}; {}",
                join_ids(held.iter().map(|(guardian_id, _)| *guardian_id)),
                join_ids(missing),
                if observed.is_some() {
                    "observer history has it"
                } else {
                    "observer history does not"
                },
            ),
        )),
        // Every responding guardian agrees, so the difference is on the
        // observer's side: it lags behind consensus, or the output is an input
        // of a peg-out that guardians already reserved but that has not reached
        // threshold signatures (guardian summaries do not list those inputs).
        (true, true, false) => Some((
            GuardianUtxoDisagreementKind::ObserverDifference,
            "Observer history has this output unspent, but no responding guardian lists it; it may be an input of a peg-out still collecting signatures".to_owned(),
        )),
        (false, false, true) => Some((
            GuardianUtxoDisagreementKind::ObserverDifference,
            "All responding guardians report this output, but observer history does not have it yet".to_owned(),
        )),
        _ => None,
    }
}

fn join_ids(guardian_ids: impl IntoIterator<Item = u16>) -> String {
    guardian_ids
        .into_iter()
        .map(|guardian_id| guardian_id.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

async fn get_federation_totals(
    State(state): State<AppState>,
) -> crate::error::Result<Json<FedimintTotals>> {
    Ok(state.federation_observer.totals().await?.into())
}

#[cfg(test)]
mod utxo_tests {
    use fmo_api_types::GuardianClaimedUtxo;

    use super::*;

    fn observed() -> FederationUtxo {
        FederationUtxo {
            out_point: OutPoint::null(),
            address: bitcoin::Address::p2wsh(&bitcoin::ScriptBuf::new(), bitcoin::Network::Bitcoin)
                .as_unchecked()
                .clone(),
            amount: Amount::from_sats(100),
        }
    }

    fn claim(id: u16) -> GuardianUtxoClaim {
        GuardianUtxoClaim {
            guardian_id: id,
            status: GuardianUtxoClaimStatus::Ok,
            session_count: Some(100),
            error: None,
            utxos: vec![GuardianClaimedUtxo {
                out_point: OutPoint::null(),
                amount: Amount::from_sats(100),
                state: GuardianClaimedUtxoState::Spendable,
            }],
        }
    }

    #[test]
    fn omitted_utxo_remains_an_inventory_difference() {
        let mut missing = claim(1);
        missing.utxos.clear();
        let differences = guardian_utxo_disagreements(&[observed()], &[claim(0), missing]);
        assert_eq!(differences.len(), 1);
        assert_eq!(
            differences[0].kind,
            GuardianUtxoDisagreementKind::InventoryDifference
        );
    }

    #[test]
    fn false_amount_is_an_evidence_mismatch() {
        let mut false_claim = claim(0);
        false_claim.utxos[0].amount = Amount::from_sats(999);
        let differences = guardian_utxo_disagreements(&[observed()], &[false_claim]);
        assert!(differences
            .iter()
            .any(|difference| difference.kind == GuardianUtxoDisagreementKind::EvidenceMismatch));
    }

    #[test]
    fn unsigned_change_is_not_a_missing_spendable_utxo() {
        let mut pending = claim(0);
        pending.utxos[0].state = GuardianClaimedUtxoState::UnsignedChange;
        assert!(guardian_utxo_disagreements(&[], &[pending]).is_empty());
    }

    #[test]
    fn unavailable_guardian_does_not_create_a_false_disagreement() {
        let unavailable = GuardianUtxoClaim {
            guardian_id: 1,
            status: GuardianUtxoClaimStatus::Unavailable,
            session_count: None,
            utxos: Vec::new(),
            error: Some("Guardian does not expose wallet summary".to_owned()),
        };

        assert!(guardian_utxo_disagreements(&[observed()], &[claim(0), unavailable]).is_empty());
    }

    #[test]
    fn matching_guardian_claims_do_not_create_a_disagreement() {
        assert!(guardian_utxo_disagreements(&[observed()], &[claim(0), claim(1)]).is_empty());
    }

    #[test]
    fn guardian_amount_conflict_is_detected_without_observer_history() {
        let mut conflicting = claim(1);
        conflicting.utxos[0].amount = Amount::from_sats(999);
        let differences = guardian_utxo_disagreements(&[], &[claim(0), conflicting]);
        assert!(differences
            .iter()
            .any(|difference| difference.kind == GuardianUtxoDisagreementKind::EvidenceMismatch));
    }

    #[test]
    fn guardian_still_seeing_unsigned_change_is_not_missing_it() {
        let mut behind = claim(1);
        behind.utxos[0].state = GuardianClaimedUtxoState::UnsignedChange;
        assert!(guardian_utxo_disagreements(&[observed()], &[claim(0), behind]).is_empty());
    }

    #[test]
    fn lagging_guardian_is_left_out_of_the_comparison() {
        let mut lagging = claim(1);
        lagging.status = GuardianUtxoClaimStatus::Lagging;
        lagging.utxos.clear();
        assert!(guardian_utxo_disagreements(&[observed()], &[claim(0), lagging]).is_empty());
    }

    #[test]
    fn unconfirmed_change_matches_observed_change() {
        let mut change = claim(0);
        change.utxos[0].state = GuardianClaimedUtxoState::UnconfirmedChange;
        assert!(guardian_utxo_disagreements(&[observed()], &[change, claim(1)]).is_empty());
    }

    #[test]
    fn output_no_guardian_lists_is_an_observer_difference() {
        let mut empty = claim(0);
        empty.utxos.clear();
        let differences = guardian_utxo_disagreements(&[observed()], &[empty]);
        assert_eq!(differences.len(), 1);
        assert_eq!(
            differences[0].kind,
            GuardianUtxoDisagreementKind::ObserverDifference
        );
    }

    #[test]
    fn output_only_guardians_report_is_an_observer_difference() {
        let differences = guardian_utxo_disagreements(&[], &[claim(0), claim(1)]);
        assert_eq!(differences.len(), 1);
        assert_eq!(
            differences[0].kind,
            GuardianUtxoDisagreementKind::ObserverDifference
        );
    }

    #[test]
    fn guardian_split_is_reported_once_per_output() {
        let mut missing = claim(1);
        missing.utxos.clear();
        let mut also_missing = claim(2);
        also_missing.utxos.clear();
        let differences = guardian_utxo_disagreements(&[], &[claim(0), missing, also_missing]);
        assert_eq!(differences.len(), 1);
        assert_eq!(
            differences[0].kind,
            GuardianUtxoDisagreementKind::InventoryDifference
        );
    }
}

async fn publish_rating_event(
    State(state): State<AppState>,
    Json(event): Json<nostr_sdk::Event>,
) -> crate::error::Result<()> {
    Ok(state.federation_observer.submit_rating(event).await?)
}

#[derive(Deserialize, Debug)]
struct BackfillParams {
    session_start: Option<i32>,
    session_end: Option<i32>,
}

async fn backfill_federation(
    Path(federation_id): Path<FederationId>,
    AuthBearer(auth): AuthBearer,
    State(state): State<AppState>,
    Json(params): Json<BackfillParams>,
) -> crate::error::Result<()> {
    state.federation_observer.check_auth(&auth)?;

    Ok(state
        .federation_observer
        .backfill_federation(federation_id, params.session_start, params.session_end)
        .await?)
}

fn decoders_from_config(config: &ClientConfig) -> ModuleDecoderRegistry {
    get_decoders(
        config
            .modules
            .iter()
            .map(|(module_instance_id, module_config)| {
                (*module_instance_id, module_config.kind.clone())
            }),
    )
    .with_fallback()
}

fn instance_to_kind(config: &ClientConfig, module_instance_id: ModuleInstanceId) -> String {
    config
        .modules
        .get(&module_instance_id)
        .map(|module_config| module_config.kind.to_string())
        .unwrap_or_else(|| "not-in-config".to_owned())
}

async fn get_nonces_spend_info(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
    Json(request): Json<NoncesRequest>,
) -> crate::error::Result<Json<std::collections::HashMap<String, NonceSpendInfo>>> {
    Ok(state
        .federation_observer
        .get_nonces_spend_info(federation_id, &request.nonces)
        .await?
        .into())
}
