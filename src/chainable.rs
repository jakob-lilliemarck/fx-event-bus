use sqlx::PgTransaction;

/// Trait for types that can create transaction-aware versions of themselves
pub trait FromOther<'tx> {
    type TxType: 'tx + Into<PgTransaction<'tx>>;

    fn from(
        &'tx self,
        other: impl Into<PgTransaction<'tx>>,
    ) -> Self::TxType;
}