//! Project stored JSON without allocating Values for discarded fields.
//! Skipped values are still parsed through deserialize_any (not IgnoredAny),
//! preserving number-range, UTF-8, syntax and recursion-limit validation.
use crate::index::Document;
use serde::de::{self, Deserialize, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::{borrow::Cow, collections::HashSet, fmt};

pub(super) fn decode(
    bytes: &[u8],
    fields: &HashSet<String>,
    stored_names: &[String],
) -> serde_json::Result<Document> {
    if fields.is_empty() {
        return serde_json::from_slice(bytes);
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let document = Projection { fields, stored_names }.deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(document)
}

struct Projection<'a> {
    fields: &'a HashSet<String>,
    stored_names: &'a [String],
}
impl<'de> DeserializeSeed<'de> for Projection<'_> {
    type Value = Document;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Document, D::Error> {
        deserializer.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for Projection<'_> {
    type Value = Document;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a document object")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Document, M::Error> {
        let mut document = Document::new();
        while let Some(FieldName(key)) = map.next_key::<FieldName<'de>>()? {
            // The old filter removes only known stored fields. Preserve unknown
            // keys too, even though the normal writer never emits them.
            if self.fields.contains(key.as_ref())
                || !self.stored_names.iter().any(|name| name == key.as_ref())
            {
                document.insert(key.into_owned(), map.next_value()?);
            } else {
                map.next_value::<ValidatedSkip>()?;
            }
        }
        Ok(document)
    }
}

struct FieldName<'a>(Cow<'a, str>);
impl<'de> Deserialize<'de> for FieldName<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;
        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = FieldName<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result { f.write_str("a field name") }
            fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
                Ok(FieldName(Cow::Borrowed(value)))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(FieldName(Cow::Owned(value.to_owned())))
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(FieldName(Cow::Owned(value)))
            }
        }
        deserializer.deserialize_str(KeyVisitor)
    }
}

struct ValidatedSkip;
impl<'de> Deserialize<'de> for ValidatedSkip {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(Self)
    }
}
impl<'de> Visitor<'de> for ValidatedSkip {
    type Value = Self;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result { f.write_str("a JSON value") }
    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self, E> { Ok(self) }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self, E> { Ok(self) }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self, E> { Ok(self) }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self, E> { Ok(self) }
    fn visit_str<E: de::Error>(self, _: &str) -> Result<Self, E> { Ok(self) }
    fn visit_unit<E: de::Error>(self) -> Result<Self, E> { Ok(self) }
    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self, S::Error> {
        while seq.next_element::<Self>()?.is_some() {}
        Ok(self)
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self, M::Error> {
        while map.next_key::<FieldName<'de>>()?.is_some() { map.next_value::<Self>()?; }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reference(bytes: &[u8], fields: &HashSet<String>, stored: &[String]) -> serde_json::Result<Document> {
        let mut doc: Document = serde_json::from_slice(bytes)?;
        if !fields.is_empty() {
            for key in stored { if !fields.contains(key) { doc.shift_remove(key); } }
        }
        Ok(doc)
    }
    fn compare(bytes: &[u8], fields: &[&str], stored: &[&str]) {
        let fields = fields.iter().map(|s| s.to_string()).collect();
        let stored: Vec<_> = stored.iter().map(|s| s.to_string()).collect();
        let old = reference(bytes, &fields, &stored);
        let new = decode(bytes, &fields, &stored);
        assert_eq!(old.is_ok(), new.is_ok(), "old={old:?} new={new:?}");
        if let (Ok(old), Ok(new)) = (old, new) {
            assert_eq!(serde_json::to_vec(&old).unwrap(), serde_json::to_vec(&new).unwrap());
        }
    }
    #[test]
    fn projected_values_order_duplicates_and_unknown_fields_match() {
        for bytes in [
            br#"{}"#.as_slice(),
            br#"{"body":"large","docid":"001","nested":{"b":[true,null,1,-0.0,1.5],"a":"x"}}"#,
            br#"{"docid":1,"body":"x","docid":2,"unknown":"preserve","nested":[]}"#,
            br#"{"body":"x","doc\u0069d":"escaped key","nested":{"surrogate":"\uD83E\uDD80"}}"#,
            "{\"body\":\"中文🦀\\n\\u0000\",\"docid\":18446744073709551615,\"nested\":-9223372036854775808}".as_bytes(),
        ] {
            for fields in [vec![], vec!["docid"], vec!["nested","docid"], vec!["missing"], vec!["body","docid","nested"]] {
                compare(bytes,&fields,&["body","docid","nested"]);
            }
        }
    }
    #[test]
    fn discarded_values_keep_json_validation() {
        for bytes in [
            br#"{"docid":1,"body":1e400}"#.as_slice(),
            br#"{"body":"\uD800","docid":1}"#,
            br#"{"body":[1,],"docid":1}"#,
            br#"{"docid":1} true"#,
            b"{\"docid\":1,\"body\":\"\xff\"}",
            br#"{"docid":1,"body":{"x":}}"#,
            b"[]", b"null", b"{", b"",
        ] { compare(bytes,&["docid"],&["body","docid"]); }
        for depth in [0,1,64,126,127,128,129,256] {
            let text = format!("{{\"docid\":1,\"body\":{}0{}}}","[".repeat(depth),"]".repeat(depth));
            compare(text.as_bytes(),&["docid"],&["body","docid"]);
        }
    }
}
