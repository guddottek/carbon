use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use carbon_core::{
    datasource::{Datasource, DatasourceId, TransactionUpdate, Update, UpdateType},
    error::CarbonResult,
};
use carbon_jito_protos::shredstream::{
    SubscribeEntriesRequest, shredstream_proxy_client::ShredstreamProxyClient,
};
use futures::{TryStreamExt, stream::try_unfold};
use scc::HashCache;
use solana_address::Address;
use solana_entry::entry::Entry;
use solana_message::v0::LoadedAddresses;
use solana_rpc_client::rpc_client::SerializableTransaction;
use solana_transaction_status_client_types::TransactionStatusMeta;
use tokio_util::sync::CancellationToken;

type LocalAddressLookupTables = Arc<HashCache<Address, Vec<Address>>>;

#[derive(Debug)]
pub struct JitoShredstreamGrpcClient {
    endpoint: String,
    local_address_lookup_tables: Option<LocalAddressLookupTables>,
    include_vote: bool,
}

impl JitoShredstreamGrpcClient {
    pub fn new(
        endpoint: String,
        local_address_lookup_tables: Option<LocalAddressLookupTables>,
        include_vote: bool,
    ) -> Self {
        JitoShredstreamGrpcClient {
            endpoint,
            local_address_lookup_tables,
            include_vote,
        }
    }
}

#[async_trait]
impl Datasource for JitoShredstreamGrpcClient {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Update, DatasourceId)>,
        cancellation_token: CancellationToken,
    ) -> CarbonResult<()> {
        let mut client = ShredstreamProxyClient::connect(self.endpoint.clone())
            .await
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?;
        let local_address_lookup_tables = self.local_address_lookup_tables.clone();
        let include_vote = self.include_vote;

        tokio::spawn(async move {
            let result = tokio::select! {
                _ = cancellation_token.cancelled() => {
                    log::info!("Cancelling Jito Shreadstream gRPC subscription.");
                    return;
                }

                result = client.subscribe_entries(SubscribeEntriesRequest {}) =>
                    result
            };

            let stream = match result {
                Ok(r) => r.into_inner(),
                Err(e) => {
                    log::error!("Failed to subscribe: {e:?}");
                    return;
                }
            };

            let stream = try_unfold(
                (stream, cancellation_token),
                |(mut stream, cancellation_token)| async move {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            log::info!("Cancelling Jito Shreadstream gRPC subscription.");
                            Ok(None)
                        },
                        v = stream.message() => match v {
                            Ok(Some(v)) => Ok(Some((v, (stream, cancellation_token)))),
                            Ok(None) => Ok(None),
                            Err(e) => Err(e),
                        },
                    }
                },
            );

            let dedup_cache = Arc::new(HashCache::with_capacity(1024, 4096));

            if let Err(e) = stream
                .try_for_each_concurrent(None, |message| {
                    let sender = sender.clone();
                    let local_address_lookup_tables = local_address_lookup_tables.clone();
                    let dedup_cache = dedup_cache.clone();
                    let id_for_closure = id.clone();

                    async move {
                        let start_time = SystemTime::now();
                        let block_time = Some(
                            start_time
                                .duration_since(UNIX_EPOCH)
                                .expect("Time")
                                .as_secs() as i64,
                        );

                        let entries: Vec<Entry> = match bincode::deserialize(&message.entries) {
                            Ok(e) => e,
                            Err(e) => {
                                log::error!("Failed to deserialize entries: {e:?}");
                                return Ok(());
                            }
                        };

                        for entry in entries {
                            if dedup_cache.contains_async(&entry.hash).await {
                                continue;
                            }
                            let _ = dedup_cache.put_async(entry.hash, ()).await;

                            for transaction in entry.transactions {
                                let accounts = transaction.message.static_account_keys();
                                let is_vote = accounts.len() == 3 && solana_sdk_ids::vote::check_id(&accounts[2]);
                                if !include_vote && is_vote {
                                    continue;
                                }

                                if transaction.signatures.is_empty() {
                                    log::error!("transaction.signatures.is_empty: {:?}", transaction);
                                    return Ok(());
                                }

                                let signature = *transaction.get_signature();

                                let mut loaded_addresses = LoadedAddresses::default();

                                if let (Some(local_address_lookup_tables), Some(tables)) = (
                                    &local_address_lookup_tables,
                                    transaction.message.address_table_lookups(),
                                ) {
                                    for table in tables.iter() {
                                        let keys = local_address_lookup_tables
                                            .get_async(&table.account_key)
                                            .await;

                                        if let Some(keys) = keys {
                                            let keys = keys.get();

                                            let writable: Vec<_> = table
                                                .writable_indexes
                                                .iter()
                                                .filter_map(|&i| keys.get(i as usize))
                                                .collect();
                                            let readonly: Vec<_> = table
                                                .readonly_indexes
                                                .iter()
                                                .filter_map(|&i| keys.get(i as usize))
                                                .collect();

                                            loaded_addresses.writable.extend(writable);
                                            loaded_addresses.readonly.extend(readonly);
                                        }
                                    }
                                }

                                let update = Update::Transaction(Box::new(TransactionUpdate {
                                    signature,
                                    is_vote,
                                    transaction,
                                    meta: TransactionStatusMeta {
                                        status: Ok(()),
                                        loaded_addresses,
                                        ..Default::default()
                                    },
                                    slot: message.slot,
                                    index: None,
                                    block_time,
                                    block_hash: None,
                                }));

                                if let Err(e) = sender.try_send((update, id_for_closure.clone())) {
                                    log::error!(
                                        "Failed to send transaction update with signature {:?} at slot {}: {:?}",
                                        signature,
                                        message.slot,
                                        e
                                    );
                                    return Ok(());
                                }
                            }
                        }

                        Ok(())
                    }
                })
                .await
            {
                log::error!("Grpc stream error: {e:?}");
            }
        });

        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        vec![UpdateType::Transaction]
    }
}
