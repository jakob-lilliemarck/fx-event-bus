use super::EventHandlingError;
use crate::{chainable::FromOther, models::Event};
use serde::de::DeserializeOwned;
use std::future::Future;

pub trait EventHandler {
    type Input: Event + DeserializeOwned;

    fn handle(
        &mut self,
        input: &Self::Input,
    ) -> impl Future<Output = Result<(), EventHandlingError>> + Send;
}

pub trait TxEventHandler<'tx>: FromOther<'tx> {
    type Input: Event + DeserializeOwned;

    fn handle(
        &mut self,
        input: &Self::Input,
    ) -> impl Future<Output = Result<(), EventHandlingError>> + Send;
}
