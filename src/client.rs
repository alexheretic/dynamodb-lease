use crate::{ClientBuilder, Lease, local::LocalLocks};
use anyhow::{Context, bail, ensure};
use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{
        delete_item::{DeleteItemError, DeleteItemOutput},
        put_item::PutItemError,
        update_item::UpdateItemError,
    },
    types::{AttributeValue, KeyType, ScalarAttributeType},
};
use aws_smithy_runtime_api::client::orchestrator;
use std::{
    cmp::min,
    sync::Arc,
    time::{Duration, Instant},
};
use time::OffsetDateTime;
use tracing::instrument;
use uuid::Uuid;

const KEY_FIELD: &str = "key";
const LEASE_EXPIRY_FIELD: &str = "lease_expiry";
const LEASE_VERSION_FIELD: &str = "lease_version";

/// Client for acquiring [`Lease`]s.
///
/// Communicates with dynamodb to acquire, extend and delete distributed leases.
///
/// Local mutex locks are also used to eliminate db contention for usage within
/// a single `Client` instance or clone.
#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) client: aws_sdk_dynamodb::Client,
    pub(crate) table_name: Arc<String>,
    pub(crate) lease_ttl_seconds: u32,
    pub(crate) extend_period: Duration,
    pub(crate) acquire_cooldown: Duration,
    pub(crate) grace_period: Duration,
    pub(crate) local_locks: LocalLocks,
}

impl Client {
    /// Returns a new [`Client`] builder.
    pub fn builder() -> ClientBuilder {
        <_>::default()
    }

    /// Tries to acquire a new [`Lease`] for the given `key`.
    ///
    /// If this lease has already been acquired elsewhere `Ok(None)` is returned.
    ///
    /// Does not wait to acquire a lease, to do so see [`Client::acquire`].
    #[instrument(skip_all)]
    pub async fn try_acquire(&self, key: impl Into<String>) -> anyhow::Result<Option<Lease>> {
        let key = key.into();
        let local_guard = match self.local_locks.try_lock(key.clone()) {
            Ok(g) => g,
            Err(_) => return Ok(None),
        };

        match self.put_lease(key).await {
            Ok(Some(lease)) => Ok(Some(lease.with_local_guard(local_guard))),
            x => x,
        }
    }

    /// Acquires a new [`Lease`] for the given `key`. May wait until successful if the lease
    /// has already been acquired elsewhere.
    ///
    /// To try to acquire without waiting see [`Client::try_acquire`].
    #[instrument(skip_all)]
    pub async fn acquire(&self, key: impl Into<String>) -> anyhow::Result<Lease> {
        let key = key.into();
        let local_guard = self.local_locks.lock(key.clone()).await;

        loop {
            if let Some(lease) = self.put_lease(key.clone()).await? {
                return Ok(lease.with_local_guard(local_guard));
            }
            tokio::time::sleep(self.acquire_cooldown).await;
        }
    }

    /// Acquires a new [`Lease`] for the given `key`. May wait until successful if the lease
    /// has already been acquired elsewhere up to a max of `max_wait`.
    ///
    /// To try to acquire without waiting see [`Client::try_acquire`].
    #[instrument(skip_all)]
    pub async fn acquire_timeout(
        &self,
        key: impl Into<String>,
        max_wait: Duration,
    ) -> anyhow::Result<Lease> {
        let start = Instant::now();
        let key = key.into();

        let local_guard = tokio::time::timeout(max_wait, self.local_locks.lock(key.clone()))
            .await
            .context("Could not acquire within {max_wait:?}")?;

        loop {
            if let Some(lease) = self.put_lease(key.clone()).await? {
                return Ok(lease.with_local_guard(local_guard));
            }
            let elapsed = start.elapsed();
            if elapsed > max_wait {
                bail!("Could not acquire within {max_wait:?}");
            }
            let remaining_max_wait = max_wait - elapsed;
            tokio::time::sleep(min(self.acquire_cooldown, remaining_max_wait)).await;
        }
    }

    /// Acquires a new [`Lease`] for the given `key`. If an expired lease exists for the key,
    /// it attempts to remove it before acquiring. May wait until successful if the lease
    /// is actively held elsewhere.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use dynamodb_lease::Client;
    /// # use std::time::Duration;
    /// # async fn example() -> anyhow::Result<()> {
    /// #     // Assume db_client is an initialized aws_sdk_dynamodb::Client
    /// #     let db_client: aws_sdk_dynamodb::Client = unimplemented!();
    /// #     let client = Client::builder()
    /// #         .table_name("my-lease-table") // Specify your table name
    /// #         .build_and_check_db(db_client)
    /// #         .await?;
    /// // Acquire a lease, potentially cleaning up an old expired one first.
    /// let lease = client.acquire_or_replace_expired_lease("my-unique-key").await?;
    /// println!("Acquired lease: {:?}", lease);
    /// // Lease is automatically released when `lease` goes out of scope.
    /// #     Ok(())
    /// # }
    /// ```
    #[instrument(skip_all)]
    pub async fn acquire_or_replace_expired_lease(
        &self,
        key: impl Into<String>,
    ) -> anyhow::Result<Lease> {
        let key = key.into();
        let local_guard = self.local_locks.lock(key.clone()).await;

        loop {
            match self.try_acquire_or_replace_expired(&key).await {
                Ok(Some(lease)) => return Ok(lease.with_local_guard(local_guard)),
                Ok(None) => { /* Lease held or failed conditional delete, retry */ }
                Err(e) => return Err(e), // Propagate other errors
            }
            tokio::time::sleep(self.acquire_cooldown).await;
        }
    }

    // Internal helper to attempt acquiring or replacing an expired lease once.
    //
    // 1. Gets the item.
    // 2. If it exists and is expired, attempts a conditional delete.
    // 3. If delete succeeds or item didn't exist, attempts a conditional put.
    // Returns Ok(Some(Lease)) on successful acquisition.
    // Returns Ok(None) if the lease is currently held by another owner or if a
    // conditional delete/put fails (indicating a concurrent modification), suggesting a retry.
    // Returns Err(_) for other failures.
    async fn try_acquire_or_replace_expired(&self, key: &str) -> anyhow::Result<Option<Lease>> {
        let now_ts = OffsetDateTime::now_utc().unix_timestamp();
        let grace_period_secs = self.grace_period.as_secs();

        // 1. Get Item
        let get_output = self
            .client
            .get_item()
            .table_name(self.table_name.as_str())
            .key(KEY_FIELD, AttributeValue::S(key.to_string()))
            .consistent_read(true) // Ensure we read the latest state
            .send()
            .await;

        match get_output {
            Ok(output) => {
                if let Some(item) = output.item {
                    // Item exists, check expiry
                    let expiry_val = item
                        .get(LEASE_EXPIRY_FIELD)
                        .context("Missing lease_expiry field")?;
                    let maybe_version_val = item.get(LEASE_VERSION_FIELD);

                    let expiry_ts = expiry_val
                        .as_n()
                        .map_err(|_| anyhow::anyhow!("lease_expiry is not a number"))?
                        .parse::<i64>()
                        .context("Failed to parse lease_expiry")?;

                    match maybe_version_val {
                        Some(version_val) => {
                            // Version exists: Check expiry + grace period and delete based on version
                            let version_str = version_val
                                .as_s()
                                .map_err(|_| anyhow::anyhow!("lease_version is not a string"))?;

                            let expiry_with_grace = expiry_ts.saturating_add(
                                i64::try_from(grace_period_secs).unwrap_or(i64::MAX),
                            );

                            if now_ts >= expiry_with_grace {
                                // Lease is expired beyond the grace period, try conditional delete on version
                                let delete_result = self
                                    .client
                                    .delete_item()
                                    .table_name(self.table_name.as_str())
                                    .key(KEY_FIELD, AttributeValue::S(key.to_string()))
                                    .condition_expression(format!(
                                        "{LEASE_VERSION_FIELD} = :version"
                                    ))
                                    .expression_attribute_values(
                                        ":version",
                                        AttributeValue::S(version_str.to_string()),
                                    )
                                    .send()
                                    .await;
                                // Handle delete result (success -> put_lease, conditional fail -> Ok(None), error -> Err)
                                handle_delete_result(delete_result, self, key).await
                            } else {
                                // Lease exists but is not expired OR is within the grace period
                                Ok(None)
                            }
                        }
                        None => {
                            // Version missing: Check expiry (no grace) and delete based on expiry
                            tracing::warn!(
                                key = key,
                                "Lease item missing '{LEASE_VERSION_FIELD}' field, attempting expiry-based replacement (backward compatibility)"
                            );
                            if now_ts >= expiry_ts {
                                // Lease is expired (no grace period applied), try conditional delete on expiry
                                let delete_result = self
                                    .client
                                    .delete_item()
                                    .table_name(self.table_name.as_str())
                                    .key(KEY_FIELD, AttributeValue::S(key.to_string()))
                                    .condition_expression("attribute_exists(#k) AND #e = :expiry")
                                    .expression_attribute_names("#k", KEY_FIELD)
                                    .expression_attribute_names("#e", LEASE_EXPIRY_FIELD)
                                    .expression_attribute_values(
                                        ":expiry",
                                        expiry_val.clone(), // Use the original AttributeValue
                                    )
                                    .send()
                                    .await;
                                // Handle delete result (success -> put_lease, conditional fail -> Ok(None), error -> Err)
                                handle_delete_result(delete_result, self, key).await
                            } else {
                                // Lease exists (without version) but is not yet expired
                                Ok(None)
                            }
                        }
                    }
                } else {
                    // Item does not exist, try to put it
                    self.put_lease(key.to_string()).await
                }
            }
            Err(SdkError::ServiceError(se)) => {
                Err(anyhow::Error::from(se.into_err()).context("Failed to get lease item"))
            }
            Err(e) => Err(anyhow::Error::from(e).context("Failed to get lease item")),
        }
    }

    /// Put a new lease into the db.
    async fn put_lease(&self, key: String) -> anyhow::Result<Option<Lease>> {
        let expiry_timestamp =
            OffsetDateTime::now_utc().unix_timestamp() + i64::from(self.lease_ttl_seconds);
        let lease_v = Uuid::new_v4();

        let put = self
            .client
            .put_item()
            .table_name(self.table_name.as_str())
            .item(KEY_FIELD, AttributeValue::S(key.clone()))
            .item(
                LEASE_EXPIRY_FIELD,
                AttributeValue::N(expiry_timestamp.to_string()),
            )
            .item(LEASE_VERSION_FIELD, AttributeValue::S(lease_v.to_string()))
            .condition_expression(format!("attribute_not_exists({LEASE_VERSION_FIELD})"))
            .send()
            .await;

        match put {
            Err(SdkError::ServiceError(se))
                if matches!(se.err(), PutItemError::ConditionalCheckFailedException(..)) =>
            {
                Ok(None)
            }
            Err(err) => Err(err.into()),
            Ok(_) => Ok(Some(Lease::new(self.clone(), key, lease_v))),
        }
    }

    /// Delete a lease with a given `key` & `lease_v`.
    #[instrument(skip_all)]
    pub(crate) async fn delete_lease(
        &self,
        key: String,
        lease_v: Uuid,
    ) -> Result<DeleteItemOutput, SdkError<DeleteItemError, orchestrator::HttpResponse>> {
        self.client
            .delete_item()
            .table_name(self.table_name.as_str())
            .key(KEY_FIELD, AttributeValue::S(key))
            .condition_expression(format!("{LEASE_VERSION_FIELD}=:lease_v"))
            .expression_attribute_values(":lease_v", AttributeValue::S(lease_v.to_string()))
            .send()
            .await
    }

    /// Cleanup local lock memory for the given `key` if not in use.
    pub(crate) fn try_clean_local_lock(&self, key: String) {
        self.local_locks.try_remove(key)
    }

    /// Extends an active lease. Returns the new `lease_v` uuid.
    #[instrument(skip_all)]
    pub(crate) async fn extend_lease(
        &self,
        key: String,
        lease_v: Uuid,
    ) -> Result<Uuid, SdkError<UpdateItemError, orchestrator::HttpResponse>> {
        let expiry_timestamp =
            OffsetDateTime::now_utc().unix_timestamp() + i64::from(self.lease_ttl_seconds);
        let new_lease_v = Uuid::new_v4();

        self.client
            .update_item()
            .table_name(self.table_name.as_str())
            .key(KEY_FIELD, AttributeValue::S(key))
            .update_expression(format!(
                "SET {LEASE_VERSION_FIELD}=:new_lease_v, {LEASE_EXPIRY_FIELD}=:expiry"
            ))
            .condition_expression(format!("{LEASE_VERSION_FIELD}=:lease_v"))
            .expression_attribute_values(":new_lease_v", AttributeValue::S(new_lease_v.to_string()))
            .expression_attribute_values(":lease_v", AttributeValue::S(lease_v.to_string()))
            .expression_attribute_values(":expiry", AttributeValue::N(expiry_timestamp.to_string()))
            .send()
            .await?;

        Ok(new_lease_v)
    }

    /// Checks table is active & has a valid schema.
    pub(crate) async fn check_schema(&self) -> anyhow::Result<()> {
        // fetch table & ttl descriptions concurrently
        let (table_desc, ttl_desc) = tokio::join!(
            self.client
                .describe_table()
                .table_name(self.table_name.as_str())
                .send(),
            self.client
                .describe_time_to_live()
                .table_name(self.table_name.as_str())
                .send()
        );

        let desc = table_desc
            .with_context(|| format!("Missing table `{}`?", self.table_name))?
            .table
            .context("no table description")?;

        // check "key" field is a S hash key
        let attrs = desc.attribute_definitions.unwrap_or_default();
        let key_schema = desc.key_schema.unwrap_or_default();
        ensure!(
            key_schema.len() == 1,
            "Unexpected number of keys ({}) in key_schema, expected 1. Got {:?}",
            key_schema.len(),
            vec(key_schema.iter().map(|k| k.attribute_name())),
        );
        let described_kind = attrs
            .iter()
            .find(|attr| attr.attribute_name() == KEY_FIELD)
            .with_context(|| {
                format!(
                    "Missing attribute definition for {KEY_FIELD}, available {:?}",
                    vec(attrs.iter().map(|a| a.attribute_name()))
                )
            })?
            .attribute_type();
        ensure!(
            described_kind == &ScalarAttributeType::S,
            "Unexpected attribute type `{:?}` for {}, expected `{:?}`",
            described_kind,
            KEY_FIELD,
            ScalarAttributeType::S,
        );

        let described_key_type = key_schema
            .iter()
            .find(|k| k.attribute_name() == KEY_FIELD)
            .with_context(|| {
                format!(
                    "Missing key schema for {KEY_FIELD}, available {:?}",
                    vec(key_schema.iter().map(|k| k.attribute_name()))
                )
            })?
            .key_type();
        ensure!(
            described_key_type == &KeyType::Hash,
            "Unexpected key type `{:?}` for {}, expected `{:?}`",
            described_key_type,
            KEY_FIELD,
            KeyType::Hash,
        );

        // check "lease_expiry" is a ttl field
        let update_time_to_live_desc = ttl_desc
            .with_context(|| format!("Missing time_to_live for table `{}`?", self.table_name))?
            .time_to_live_description
            .context("no time to live description")?;

        ensure!(
            update_time_to_live_desc.attribute_name() == Some(LEASE_EXPIRY_FIELD),
            "time to live for {} is not set",
            LEASE_EXPIRY_FIELD,
        );

        Ok(())
    }
}

// Helper function to handle delete result logic (DRY)
async fn handle_delete_result(
    delete_result: Result<DeleteItemOutput, SdkError<DeleteItemError, orchestrator::HttpResponse>>,
    client: &Client,
    key: &str,
) -> anyhow::Result<Option<Lease>> {
    match delete_result {
        Ok(_) => {
            // Successfully deleted expired lease, now try to put the new one
            client.put_lease(key.to_string()).await
        }
        Err(SdkError::ServiceError(se))
            if matches!(
                se.err(),
                DeleteItemError::ConditionalCheckFailedException(..)
            ) =>
        {
            // Conditional check failed - someone else modified/deleted it first.
            // Return None to signal retry in the calling loop.
            Ok(None)
        }
        Err(e) => Err(anyhow::Error::from(e).context("Failed conditional delete of expired lease")),
    }
}

#[inline]
fn vec<T>(iter: impl Iterator<Item = T>) -> Vec<T> {
    iter.collect()
}
