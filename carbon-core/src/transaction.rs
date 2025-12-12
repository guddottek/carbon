use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use solana_address::Address;
use solana_hash::Hash;
use solana_message::VersionedMessage;
use solana_signature::Signature;
use solana_transaction_status_client_types::TransactionStatusMeta;

use crate::{
    collection::InstructionDecoderCollection,
    datasource::TransactionUpdate,
    error::CarbonResult,
    filter::Filter,
    instruction::{DecodedInstruction, InstructionMetadata, NestedInstruction},
    processor::Processor,
    schema::{ParsedInstruction, TransactionSchema},
    transformers::unnest_parsed_instructions,
};

#[derive(Debug, Clone, Default)]
pub struct TransactionMetadata {
    pub slot: u64,
    pub signature: Signature,
    pub fee_payer: Address,
    pub meta: TransactionStatusMeta,
    pub message: VersionedMessage,
    pub index: Option<u64>,
    pub block_time: Option<i64>,
    pub block_hash: Option<Hash>,
}

impl TryFrom<TransactionUpdate> for TransactionMetadata {
    type Error = crate::error::Error;

    fn try_from(value: TransactionUpdate) -> Result<Self, Self::Error> {
        let accounts = value.transaction.message.static_account_keys();

        Ok(TransactionMetadata {
            slot: value.slot,
            signature: value.signature,
            fee_payer: *accounts.first().ok_or(Self::Error::MissingFeePayer)?,
            meta: value.meta.clone(),
            message: value.transaction.message.clone(),
            index: value.index,
            block_time: value.block_time,
            block_hash: value.block_hash,
        })
    }
}

pub type TransactionProcessorInputType<T, U = ()> = (
    Arc<TransactionMetadata>,
    Vec<(InstructionMetadata, DecodedInstruction<T>)>,
    Option<U>,
);

pub struct TransactionPipe<T: InstructionDecoderCollection, U> {
    schema: Option<TransactionSchema<T>>,
    processor: Box<dyn Processor<InputType = TransactionProcessorInputType<T, U>> + Send + Sync>,
    filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

pub struct ParsedTransaction<I: InstructionDecoderCollection> {
    pub metadata: TransactionMetadata,
    pub instructions: Vec<ParsedInstruction<I>>,
}

impl<T: InstructionDecoderCollection, U> TransactionPipe<T, U> {
    pub fn new(
        schema: Option<TransactionSchema<T>>,
        processor: impl Processor<InputType = TransactionProcessorInputType<T, U>>
        + Send
        + Sync
        + 'static,
        filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            schema,
            processor: Box::new(processor),
            filters,
        }
    }

    fn matches_schema(&self, instructions: &[ParsedInstruction<T>]) -> Option<U>
    where
        U: DeserializeOwned,
    {
        match self.schema {
            Some(ref schema) => schema.match_schema(instructions),
            None => None,
        }
    }
}

pub fn parse_instructions<T: InstructionDecoderCollection>(
    nested_ixs: &[NestedInstruction],
) -> Vec<ParsedInstruction<T>> {
    let mut parsed_instructions: Vec<ParsedInstruction<T>> = Vec::new();

    for nested_ix in nested_ixs {
        if let Some(instruction) = T::parse_instruction(&nested_ix.instruction) {
            parsed_instructions.push(ParsedInstruction {
                program_id: nested_ix.instruction.program_id,
                instruction,
                inner_instructions: parse_instructions(&nested_ix.inner_instructions),
            });
        } else {
            for inner_ix in nested_ix.inner_instructions.iter() {
                parsed_instructions.extend(parse_instructions(std::slice::from_ref(inner_ix)));
            }
        }
    }

    parsed_instructions
}

#[async_trait]
pub trait TransactionPipes<'a>: Send + Sync {
    async fn run(
        &mut self,
        transaction_metadata: Arc<TransactionMetadata>,
        instructions: &[NestedInstruction],
    ) -> CarbonResult<()>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl<T, U> TransactionPipes<'_> for TransactionPipe<T, U>
where
    T: InstructionDecoderCollection + Sync + 'static,
    U: DeserializeOwned + Send + Sync + 'static,
{
    async fn run(
        &mut self,
        transaction_metadata: Arc<TransactionMetadata>,
        instructions: &[NestedInstruction],
    ) -> CarbonResult<()> {
        let parsed_instructions = parse_instructions(instructions);

        let matched_data = self.matches_schema(&parsed_instructions);

        let unnested_instructions =
            unnest_parsed_instructions(transaction_metadata.clone(), parsed_instructions, 0);

        self.processor
            .process((transaction_metadata, unnested_instructions, matched_data))
            .await?;

        Ok(())
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}
