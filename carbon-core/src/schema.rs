use std::{collections::HashMap, hash::Hash};

use serde::de::DeserializeOwned;
use solana_address::Address;
use solana_instruction::AccountMeta;

use crate::{collection::InstructionDecoderCollection, instruction::DecodedInstruction};

#[derive(Debug, Clone)]
pub enum SchemaNode<T: InstructionDecoderCollection> {
    Instruction(InstructionSchemaNode<T>),
    Any,
}

#[derive(Debug, Clone)]
pub struct InstructionSchemaNode<T: InstructionDecoderCollection> {
    pub ix_type: T::InstructionType,
    pub name: String,
    pub inner_instructions: Vec<SchemaNode<T>>,
}

#[derive(Debug)]
pub struct ParsedInstruction<T: InstructionDecoderCollection> {
    pub program_id: Address,
    pub instruction: DecodedInstruction<T>,
    pub inner_instructions: Vec<ParsedInstruction<T>>,
}

#[derive(Debug, Clone)]
pub struct TransactionSchema<T: InstructionDecoderCollection> {
    pub root: Vec<SchemaNode<T>>,
}

impl<T: InstructionDecoderCollection> TransactionSchema<T> {
    pub fn match_schema<U>(&self, instructions: &[ParsedInstruction<T>]) -> Option<U>
    where
        U: DeserializeOwned,
    {
        let value = serde_json::to_value(self.match_nodes(instructions)).ok()?;
        serde_json::from_value::<U>(value).ok()
    }

    pub fn match_nodes(
        &self,
        instructions: &[ParsedInstruction<T>],
    ) -> Option<HashMap<String, (T, Vec<AccountMeta>)>> {
        let mut output = HashMap::<String, (T, Vec<AccountMeta>)>::new();

        let mut node_index = 0;
        let mut instruction_index = 0;

        let mut any = false;

        while let Some(node) = self.root.get(node_index) {
            if let SchemaNode::Any = node {
                any = true;
                node_index += 1;
                continue;
            }

            let mut matched = false;

            while let Some(current_instruction) = instructions.get(instruction_index) {
                let SchemaNode::Instruction(instruction_node) = node else {
                    return None;
                };

                if current_instruction.instruction.data.get_type() != instruction_node.ix_type
                    && !any
                {
                    return None;
                }

                if current_instruction.instruction.data.get_type() != instruction_node.ix_type
                    && any
                {
                    instruction_index += 1;
                    continue;
                }

                output.insert(
                    instruction_node.name.clone(),
                    (
                        current_instruction.instruction.data.clone(),
                        current_instruction.instruction.accounts.clone(),
                    ),
                );

                if !instruction_node.inner_instructions.is_empty() {
                    let inner_output = TransactionSchema {
                        root: instruction_node.inner_instructions.clone(),
                    }
                    .match_nodes(&current_instruction.inner_instructions)?;
                    output = merge_hashmaps(output, inner_output);
                }

                instruction_index += 1;
                node_index += 1;
                any = false;
                matched = true;
                break;
            }

            if !matched {
                return None;
            }
        }

        Some(output)
    }
}

pub fn merge_hashmaps<K, V>(
    a: HashMap<K, (V, Vec<AccountMeta>)>,
    b: HashMap<K, (V, Vec<AccountMeta>)>,
) -> HashMap<K, (V, Vec<AccountMeta>)>
where
    K: Eq + Hash,
{
    let mut output = a;
    for (key, value) in b {
        output.insert(key, value);
    }
    output
}
