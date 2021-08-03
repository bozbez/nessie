use bytes::BytesMut;
use derive_more::{Deref, DerefMut, From};
use smartstring::{LazyCompact, SmartString};

use postgres::{
    error::Error,
    types::{to_sql_checked, Field, IsNull, Kind, Oid, ToSql, Type},
    Client,
};

fn fetch_oid(client: &mut Client, typename: &str) -> Result<Oid, Error> {
    Ok(client
        .query_one("SELECT oid FROM pg_type WHERE typname = $1", &[&typename])?
        .get("oid"))
}

pub trait SqlTyped {
    fn sql_name() -> String;

    fn sql_array_name() -> String {
        format!("_{}", Self::sql_name())
    }

    fn sql_type_with_oid(oid: Oid, schema: String) -> Type;

    fn sql_type(client: &mut Client, schema: &str) -> Result<Type, Error> {
        let oid = fetch_oid(client, &Self::sql_name())?;
        Ok(Self::sql_type_with_oid(oid, schema.to_owned()))
    }

    fn sql_array_type(client: &mut Client, schema: &str) -> Result<Type, Error> {
        let inner_type = Self::sql_type(client, schema)?;
        let oid = fetch_oid(client, &Self::sql_array_name())?;

        Ok(Type::new(
            Self::sql_array_name(),
            oid,
            Kind::Array(inner_type),
            schema.to_owned(),
        ))
    }
}

#[derive(Debug, Default, Hash, Clone, Eq, Ord, PartialEq, PartialOrd, From, Deref, DerefMut)]
#[from(forward)]
pub struct Unigram(SmartString<LazyCompact>);

impl Unigram {
    pub fn new() -> Self {
        Unigram(SmartString::new())
    }
}

impl ToSql for Unigram {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + 'static + Sync + Send>> {
        self.0.as_str().to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        <&str>::accepts(ty)
    }

    to_sql_checked!();
}

#[derive(Debug, ToSql, Hash, Clone, Eq, Ord, PartialEq, PartialOrd)]
#[postgres(name = "bigram")]
pub struct Bigram {
    first: Unigram,
    second: Unigram,
}

impl Bigram {
    pub fn new(first: Unigram, second: Unigram) -> Self {
        Bigram { first, second }
    }
}

impl SqlTyped for Bigram {
    fn sql_name() -> String {
        String::from("bigram")
    }

    fn sql_type_with_oid(oid: Oid, schema: String) -> Type {
        Type::new(
            Self::sql_name(),
            oid,
            Kind::Composite(vec![
                Field::new(String::from("first"), Type::TEXT),
                Field::new(String::from("second"), Type::TEXT),
            ]),
            schema,
        )
    }
}

#[derive(Debug, ToSql, Eq, Ord, PartialEq, PartialOrd)]
#[postgres(name = "seq_unigram")]
pub struct SeqUnigram {
    seq_num: i32,
    unigram: Option<Unigram>,
}

impl SeqUnigram {
    pub fn new(seq_num: i32, unigram: Option<Unigram>) -> Self {
        SeqUnigram { seq_num, unigram }
    }
}

impl SqlTyped for SeqUnigram {
    fn sql_name() -> String {
        String::from("seq_unigram")
    }

    fn sql_type_with_oid(oid: Oid, schema: String) -> Type {
        Type::new(
            Self::sql_name(),
            oid,
            Kind::Composite(vec![
                Field::new(String::from("seq_num"), Type::INT4),
                Field::new(String::from("unigram"), Type::TEXT),
            ]),
            schema,
        )
    }
}
