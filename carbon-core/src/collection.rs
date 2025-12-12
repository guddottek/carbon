use std::{fmt::Debug, hash::Hash};

use serde::Serialize;
use solana_instruction::Instruction;

use crate::instruction::DecodedInstruction;

pub trait InstructionDecoderCollection:
    Clone + Debug + Send + Sync + Eq + Hash + Serialize + 'static
{
    type InstructionType: Clone + Debug + PartialEq + Eq + Send + Sync + 'static;

    fn parse_instruction(instruction: &Instruction) -> Option<DecodedInstruction<Self>>;

    fn get_type(&self) -> Self::InstructionType;
}
