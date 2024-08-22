use crate::Client;
use std::time::Duration;

/// [`Client`] builder.
pub struct ClientBuilder {
    table_name: String,
    acquire_cooldown: Duration,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            table_name: "leases".into(),
            acquire_cooldown: Duration::from_secs(1),
        }
    }
}

impl ClientBuilder {
    /// Sets the lease table name where the lease info will be stored.
    /// The table must have the correct schema.
    ///
    /// Default `"leases"`.
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    /// Sets how long [`Client::acquire`] waits between attempts to acquire a lease.
    ///
    /// Default `1s`.
    pub fn acquire_cooldown(mut self, cooldown: Duration) -> Self {
        self.acquire_cooldown = cooldown;
        self
    }

    /// Builds a [`Client`] and checks the dynamodb table is active with the correct schema.
    ///
    /// # Panics
    /// Panics if `extend_period` is not less than `lease_ttl_seconds`.
    pub async fn build_and_check_db(
        self,
        dynamodb_client: aws_sdk_dynamodb::Client,
    ) -> anyhow::Result<Client> {
        let client = Client {
            table_name: self.table_name.into(),
            client: dynamodb_client,
            acquire_cooldown: self.acquire_cooldown,
            local_locks: <_>::default(),
        };

        client.check_schema().await?;

        Ok(client)
    }
}
