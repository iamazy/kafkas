mod consumer;

pub use consumer::ConsumerCoordinator;
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::{ApiKey, FindCoordinatorRequest},
    protocol::StrBytes,
};
use tracing::error;

use crate::{
    client::Kafka,
    error::{ConsumeError, Result},
    executor::Executor,
    metadata::Node,
    Error,
};

pub mod transaction;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CoordinatorType {
    Group,
    Transaction,
}

impl From<CoordinatorType> for i8 {
    fn from(value: CoordinatorType) -> Self {
        match value {
            CoordinatorType::Group => 0,
            CoordinatorType::Transaction => 1,
        }
    }
}

async fn find_coordinator<Exe: Executor>(
    client: &Kafka<Exe>,
    key: StrBytes,
    key_type: CoordinatorType,
) -> Result<Node> {
    if let Some(version_range) = client.version_range(ApiKey::FindCoordinatorKey) {
        let mut find_coordinator_response = client
            .find_coordinator(find_coordinator_builder(key, key_type, version_range.max)?)
            .await?;

        if find_coordinator_response.error_code.is_ok() {
            if let Some(coordinator) = find_coordinator_response.coordinators.pop() {
                Ok(Node::new(
                    coordinator.node_id,
                    coordinator.host,
                    coordinator.port,
                ))
            } else {
                Ok(Node::new(
                    find_coordinator_response.node_id,
                    find_coordinator_response.host,
                    find_coordinator_response.port,
                ))
            }
        } else {
            error!(
                "Find coordinator error: {}, message: {:?}",
                find_coordinator_response.error_code.err().unwrap(),
                find_coordinator_response.error_message
            );
            Err(ConsumeError::CoordinatorNotAvailable.into())
        }
    } else {
        Err(Error::InvalidApiRequest(ApiKey::FindCoordinatorKey))
    }
}

fn find_coordinator_builder(
    key: StrBytes,
    key_type: CoordinatorType,
    version: i16,
) -> Result<FindCoordinatorRequest> {
    let mut request = FindCoordinatorRequest::default();
    if version <= 3 {
        request.key = key;
    } else {
        request.coordinator_keys = vec![key];
    }

    if (1..=4).contains(&version) {
        request.key_type = key_type.into();
    }
    Ok(request)
}
