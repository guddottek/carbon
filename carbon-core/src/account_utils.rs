use solana_address::Address;
use solana_instruction::AccountMeta;

pub fn next_account<'a>(iter: &mut impl Iterator<Item = &'a AccountMeta>) -> Option<Address> {
    Some(iter.next()?.pubkey)
}
