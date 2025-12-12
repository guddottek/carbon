use solana_instruction::AccountMeta;

pub trait ArrangeAccounts {
    type ArrangedAccounts;

    fn arrange_accounts(accounts: &[AccountMeta]) -> Option<Self::ArrangedAccounts>;
}
