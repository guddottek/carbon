use std::{collections::HashSet, str::FromStr, sync::Arc};

use solana_sdk::{
    message::{
        AccountMeta, Instruction, VersionedMessage, compiled_instruction::CompiledInstruction,
        v0::LoadedAddresses,
    },
    pubkey::Pubkey,
};
use solana_transaction_context::TransactionReturnData;
use solana_transaction_status::{
    InnerInstruction, InnerInstructions, Reward, TransactionStatusMeta, TransactionTokenBalance,
    UiInstruction, UiLoadedAddresses, UiTransactionStatusMeta, option_serializer::OptionSerializer,
};

use crate::{
    collection::InstructionDecoderCollection,
    datasource::TransactionUpdate,
    error::{CarbonResult, Error},
    instruction::{DecodedInstruction, InstructionMetadata, MAX_INSTRUCTION_STACK_DEPTH},
    schema::ParsedInstruction,
    transaction::TransactionMetadata,
};

pub fn extract_instructions_with_metadata(
    transaction_metadata: &Arc<TransactionMetadata>,
    transaction_update: &TransactionUpdate,
) -> CarbonResult<Vec<(InstructionMetadata, Instruction)>> {
    log::trace!(
        "extract_instructions_with_metadata(transaction_metadata: {:?}, transaction_update: {:?})",
        transaction_metadata,
        transaction_update
    );

    let message = &transaction_update.transaction.message;
    let meta = &transaction_update.meta;
    let mut instructions_with_metadata = Vec::with_capacity(32);

    match message {
        VersionedMessage::Legacy(legacy) => {
            process_instructions(
                &legacy.account_keys,
                &legacy.instructions,
                &meta.inner_instructions,
                transaction_metadata,
                &mut instructions_with_metadata,
                |_, idx| legacy.is_maybe_writable(idx, None),
                |_, idx| legacy.is_signer(idx),
            );
        }
        VersionedMessage::V0(v0) => {
            let mut account_keys: Vec<Pubkey> = Vec::with_capacity(
                v0.account_keys.len()
                    + meta.loaded_addresses.writable.len()
                    + meta.loaded_addresses.readonly.len(),
            );

            account_keys.extend_from_slice(&v0.account_keys);
            account_keys.extend_from_slice(&meta.loaded_addresses.writable);
            account_keys.extend_from_slice(&meta.loaded_addresses.readonly);

            process_instructions(
                &account_keys,
                &v0.instructions,
                &meta.inner_instructions,
                transaction_metadata,
                &mut instructions_with_metadata,
                |key, _| meta.loaded_addresses.writable.contains(key),
                |_, idx| idx < v0.header.num_required_signatures as usize,
            );
        }
    }

    Ok(instructions_with_metadata)
}

fn process_instructions<F1, F2>(
    account_keys: &[Pubkey],
    instructions: &[CompiledInstruction],
    inner: &Option<Vec<InnerInstructions>>,
    transaction_metadata: &Arc<TransactionMetadata>,
    result: &mut Vec<(InstructionMetadata, Instruction)>,
    is_writable: F1,
    is_signer: F2,
) where
    F1: Fn(&Pubkey, usize) -> bool,
    F2: Fn(&Pubkey, usize) -> bool,
{
    for (i, compiled_instruction) in instructions.iter().enumerate() {
        result.push((
            InstructionMetadata {
                transaction_metadata: transaction_metadata.clone(),
                stack_height: 1,
                index: i as u32,
                absolute_path: vec![i as u8],
            },
            build_instruction(account_keys, compiled_instruction, &is_writable, &is_signer),
        ));

        if let Some(inner_instructions) = inner {
            for inner_tx in inner_instructions {
                if inner_tx.index as usize == i {
                    let mut path_stack = [0; MAX_INSTRUCTION_STACK_DEPTH];
                    path_stack[0] = inner_tx.index;
                    let mut prev_height = 0;

                    for inner_inst in &inner_tx.instructions {
                        let stack_height = inner_inst.stack_height.unwrap_or(1) as usize;
                        if stack_height > prev_height {
                            path_stack[stack_height - 1] = 0;
                        } else {
                            path_stack[stack_height - 1] += 1;
                        }

                        result.push((
                            InstructionMetadata {
                                transaction_metadata: transaction_metadata.clone(),
                                stack_height: stack_height as u32,
                                index: inner_tx.index as u32,
                                absolute_path: path_stack[..stack_height].into(),
                            },
                            build_instruction(
                                account_keys,
                                &inner_inst.instruction,
                                &is_writable,
                                &is_signer,
                            ),
                        ));

                        prev_height = stack_height;
                    }
                }
            }
        }
    }
}

fn build_instruction<F1, F2>(
    account_keys: &[Pubkey],
    instruction: &CompiledInstruction,
    is_writable: &F1,
    is_signer: &F2,
) -> Instruction
where
    F1: Fn(&Pubkey, usize) -> bool,
    F2: Fn(&Pubkey, usize) -> bool,
{
    let program_id = *account_keys
        .get(instruction.program_id_index as usize)
        .unwrap_or(&Pubkey::default());

    let accounts = instruction
        .accounts
        .iter()
        .filter_map(|account_idx| {
            account_keys
                .get(*account_idx as usize)
                .map(|key| AccountMeta {
                    pubkey: *key,
                    is_writable: is_writable(key, *account_idx as usize),
                    is_signer: is_signer(key, *account_idx as usize),
                })
        })
        .collect();

    Instruction {
        program_id,
        accounts,
        data: instruction.data.clone(),
    }
}

pub fn extract_account_metas(
    compiled_instruction: &CompiledInstruction,
    message: &VersionedMessage,
) -> CarbonResult<Vec<AccountMeta>> {
    log::trace!(
        "extract_account_metas(compiled_instruction: {:?}, message: {:?})",
        compiled_instruction,
        message
    );
    let mut accounts = Vec::<AccountMeta>::with_capacity(compiled_instruction.accounts.len());

    for account_index in compiled_instruction.accounts.iter() {
        accounts.push(AccountMeta {
            pubkey: *message
                .static_account_keys()
                .get(*account_index as usize)
                .ok_or(Error::MissingAccountInTransaction)?,
            is_signer: message.is_signer(*account_index as usize),
            is_writable: message.is_maybe_writable(
                *account_index as usize,
                Some(
                    &message
                        .static_account_keys()
                        .iter()
                        .copied()
                        .collect::<HashSet<_>>(),
                ),
            ),
        });
    }

    Ok(accounts)
}

pub fn unnest_parsed_instructions<T: InstructionDecoderCollection>(
    transaction_metadata: Arc<TransactionMetadata>,
    instructions: Vec<ParsedInstruction<T>>,
    stack_height: u32,
) -> Vec<(InstructionMetadata, DecodedInstruction<T>)> {
    log::trace!(
        "unnest_parsed_instructions(instructions: {:?})",
        instructions
    );

    let mut result = Vec::new();

    for (ix_idx, parsed_instruction) in instructions.into_iter().enumerate() {
        result.push((
            InstructionMetadata {
                transaction_metadata: transaction_metadata.clone(),
                stack_height,
                index: ix_idx as u32 + 1,
                absolute_path: vec![],
            },
            parsed_instruction.instruction,
        ));
        result.extend(unnest_parsed_instructions(
            transaction_metadata.clone(),
            parsed_instruction.inner_instructions,
            stack_height + 1,
        ));
    }

    result
}

pub fn transaction_metadata_from_original_meta(
    meta_original: UiTransactionStatusMeta,
) -> CarbonResult<TransactionStatusMeta> {
    log::trace!(
        "transaction_metadata_from_original_meta(meta_original: {:?})",
        meta_original
    );
    Ok(TransactionStatusMeta {
        status: meta_original.status.map_err(Into::into),
        fee: meta_original.fee,
        pre_balances: meta_original.pre_balances,
        post_balances: meta_original.post_balances,
        inner_instructions: Some(
            meta_original
                .inner_instructions
                .unwrap_or_else(std::vec::Vec::new)
                .iter()
                .map(|inner_instruction_group| InnerInstructions {
                    index: inner_instruction_group.index,
                    instructions: inner_instruction_group
                        .instructions
                        .iter()
                        .map(|ui_instruction| match ui_instruction {
                            UiInstruction::Compiled(compiled_ui_instruction) => {
                                let decoded_data =
                                    bs58::decode(compiled_ui_instruction.data.clone())
                                        .into_vec()
                                        .unwrap_or_else(|_| vec![]);
                                InnerInstruction {
                                    instruction: CompiledInstruction {
                                        program_id_index: compiled_ui_instruction.program_id_index,
                                        accounts: compiled_ui_instruction.accounts.clone(),
                                        data: decoded_data,
                                    },
                                    stack_height: compiled_ui_instruction.stack_height,
                                }
                            }
                            _ => {
                                log::error!("Unsupported instruction type encountered");
                                InnerInstruction {
                                    instruction: CompiledInstruction {
                                        program_id_index: 0,
                                        accounts: vec![],
                                        data: vec![],
                                    },
                                    stack_height: None,
                                }
                            }
                        })
                        .collect::<Vec<InnerInstruction>>(),
                })
                .collect::<Vec<InnerInstructions>>(),
        ),
        log_messages: Some(
            meta_original
                .log_messages
                .unwrap_or_else(std::vec::Vec::new),
        ),
        pre_token_balances: Some(
            meta_original
                .pre_token_balances
                .unwrap_or_else(std::vec::Vec::new)
                .iter()
                .filter_map(|transaction_token_balance| {
                    if let (OptionSerializer::Some(owner), OptionSerializer::Some(program_id)) = (
                        transaction_token_balance.owner.as_ref(),
                        transaction_token_balance.program_id.as_ref(),
                    ) {
                        Some(TransactionTokenBalance {
                            account_index: transaction_token_balance.account_index,
                            mint: transaction_token_balance.mint.clone(),
                            ui_token_amount: transaction_token_balance.ui_token_amount.clone(),
                            owner: owner.to_string(),
                            program_id: program_id.to_string(),
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<TransactionTokenBalance>>(),
        ),
        post_token_balances: Some(
            meta_original
                .post_token_balances
                .unwrap_or_else(std::vec::Vec::new)
                .iter()
                .filter_map(|transaction_token_balance| {
                    if let (OptionSerializer::Some(owner), OptionSerializer::Some(program_id)) = (
                        transaction_token_balance.owner.as_ref(),
                        transaction_token_balance.program_id.as_ref(),
                    ) {
                        Some(TransactionTokenBalance {
                            account_index: transaction_token_balance.account_index,
                            mint: transaction_token_balance.mint.clone(),
                            ui_token_amount: transaction_token_balance.ui_token_amount.clone(),
                            owner: owner.to_string(),
                            program_id: program_id.to_string(),
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<TransactionTokenBalance>>(),
        ),
        rewards: Some(
            meta_original
                .rewards
                .unwrap_or_else(std::vec::Vec::new)
                .iter()
                .map(|rewards| Reward {
                    pubkey: rewards.pubkey.clone(),
                    lamports: rewards.lamports,
                    post_balance: rewards.post_balance,
                    reward_type: rewards.reward_type,
                    commission: rewards.commission,
                })
                .collect::<Vec<Reward>>(),
        ),
        loaded_addresses: {
            let loaded = meta_original
                .loaded_addresses
                .unwrap_or_else(|| UiLoadedAddresses {
                    writable: vec![],
                    readonly: vec![],
                });
            LoadedAddresses {
                writable: loaded
                    .writable
                    .iter()
                    .map(|w| Pubkey::from_str(w).unwrap_or_default())
                    .collect::<Vec<Pubkey>>(),
                readonly: loaded
                    .readonly
                    .iter()
                    .map(|r| Pubkey::from_str(r).unwrap_or_default())
                    .collect::<Vec<Pubkey>>(),
            }
        },
        return_data: meta_original
            .return_data
            .map(|return_data| TransactionReturnData {
                program_id: return_data.program_id.parse().unwrap_or_default(),
                data: return_data.data.0.as_bytes().to_vec(),
            }),
        compute_units_consumed: meta_original
            .compute_units_consumed
            .map(|compute_unit_consumed| compute_unit_consumed)
            .or(None),
        cost_units: meta_original.cost_units.into(),
    })
}
