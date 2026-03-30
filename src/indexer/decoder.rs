use crate::idl::{AnchorIdl, IdlField, IdlType, IdlTypeDefTy};
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::warn;

// ---------------------------------------------------------------------------
// Discriminator matching
// ---------------------------------------------------------------------------

/// Match the first 8 bytes of instruction data against IDL discriminators.
pub fn match_instruction<'a>(
    data: &[u8],
    idl: &'a AnchorIdl,
) -> Option<(&'a crate::idl::IdlInstruction, &'a [u8])> {
    if data.len() < 8 {
        return None;
    }
    let disc = &data[..8];
    idl.instructions
        .iter()
        .find(|ix| ix.discriminator == disc)
        .map(|ix| (ix, &data[8..]))
}

// ---------------------------------------------------------------------------
// Dynamic Borsh decoder
// ---------------------------------------------------------------------------

/// Cursor over a byte slice for sequential Borsh-compatible reading.
struct BorshReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BorshReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_bytes(&mut self, n: usize) -> anyhow::Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            anyhow::bail!(
                "Borsh overflow: need {n} at offset {}, have {}",
                self.pos,
                self.remaining()
            );
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_u8(&mut self) -> anyhow::Result<u8> { Ok(self.read_bytes(1)?[0]) }
    fn read_bool(&mut self) -> anyhow::Result<bool> { Ok(self.read_u8()? != 0) }
    fn read_u16(&mut self) -> anyhow::Result<u16> {
        Ok(u16::from_le_bytes(self.read_bytes(2)?.try_into().unwrap()))
    }
    fn read_u32(&mut self) -> anyhow::Result<u32> {
        Ok(u32::from_le_bytes(self.read_bytes(4)?.try_into().unwrap()))
    }
    fn read_u64(&mut self) -> anyhow::Result<u64> {
        Ok(u64::from_le_bytes(self.read_bytes(8)?.try_into().unwrap()))
    }
    fn read_u128(&mut self) -> anyhow::Result<u128> {
        Ok(u128::from_le_bytes(self.read_bytes(16)?.try_into().unwrap()))
    }
    fn read_i8(&mut self) -> anyhow::Result<i8> { Ok(self.read_u8()? as i8) }
    fn read_i16(&mut self) -> anyhow::Result<i16> { Ok(self.read_u16()? as i16) }
    fn read_i32(&mut self) -> anyhow::Result<i32> { Ok(self.read_u32()? as i32) }
    fn read_i64(&mut self) -> anyhow::Result<i64> { Ok(self.read_u64()? as i64) }
    fn read_i128(&mut self) -> anyhow::Result<i128> { Ok(self.read_u128()? as i128) }
    fn read_f32(&mut self) -> anyhow::Result<f32> {
        Ok(f32::from_le_bytes(self.read_bytes(4)?.try_into().unwrap()))
    }
    fn read_f64(&mut self) -> anyhow::Result<f64> {
        Ok(f64::from_le_bytes(self.read_bytes(8)?.try_into().unwrap()))
    }
    fn read_string(&mut self) -> anyhow::Result<String> {
        let len = self.read_u32()? as usize;
        Ok(String::from_utf8_lossy(self.read_bytes(len)?).into_owned())
    }
    fn read_pubkey(&mut self) -> anyhow::Result<String> {
        Ok(bs58::encode(self.read_bytes(32)?).into_string())
    }
}

/// Decode a list of IDL fields from raw bytes into a JSON object.
pub fn decode_fields(
    data: &[u8],
    fields: &[IdlField],
    type_map: &HashMap<String, &crate::idl::IdlTypeDef>,
) -> anyhow::Result<Value> {
    let mut reader = BorshReader::new(data);
    let mut map = serde_json::Map::new();
    for field in fields {
        match decode_type(&mut reader, &field.field_type, type_map) {
            Ok(val) => { map.insert(field.name.clone(), val); }
            Err(e) => {
                anyhow::bail!("Failed to decode field '{}': {}", field.name, e);
            }
        }
    }
    Ok(Value::Object(map))
}

fn decode_type(
    r: &mut BorshReader<'_>,
    ty: &IdlType,
    tm: &HashMap<String, &crate::idl::IdlTypeDef>,
) -> anyhow::Result<Value> {
    match ty {
        IdlType::Primitive(p) => decode_primitive(r, p),
        IdlType::Option { option } => {
            if r.read_u8()? == 0 { Ok(Value::Null) } else { decode_type(r, option, tm) }
        }
        IdlType::Vec { vec } => {
            let len = r.read_u32()? as usize;
            let mut arr = Vec::with_capacity(len.min(10_000));
            for _ in 0..len { arr.push(decode_type(r, vec, tm)?); }
            Ok(Value::Array(arr))
        }
        IdlType::Array { array: (inner, n) } => {
            let mut arr = Vec::with_capacity(*n);
            for _ in 0..*n { arr.push(decode_type(r, inner, tm)?); }
            Ok(Value::Array(arr))
        }
        IdlType::Defined { defined } => {
            if let Some(td) = tm.get(&defined.name) {
                match &td.type_def {
                    IdlTypeDefTy::Struct { fields } => {
                        let mut map = serde_json::Map::new();
                        for f in fields {
                            map.insert(f.name.clone(), decode_type(r, &f.field_type, tm)?);
                        }
                        Ok(Value::Object(map))
                    }
                    IdlTypeDefTy::Enum { variants } => {
                        let idx = r.read_u8()? as usize;
                        if idx >= variants.len() {
                            return Ok(json!({ "variant": idx }));
                        }
                        let v = &variants[idx];
                        match &v.fields {
                            Some(crate::idl::IdlEnumFields::Named(fields)) => {
                                let mut map = serde_json::Map::new();
                                for f in fields {
                                    map.insert(f.name.clone(), decode_type(r, &f.field_type, tm)?);
                                }
                                Ok(json!({ &v.name: map }))
                            }
                            Some(crate::idl::IdlEnumFields::Tuple(types)) => {
                                let mut arr = Vec::with_capacity(types.len());
                                for t in types {
                                    arr.push(decode_type(r, t, tm)?);
                                }
                                Ok(json!({ &v.name: arr }))
                            }
                            None => Ok(json!(&v.name)),
                        }
                    }
                }
            } else {
                Ok(json!(format!("<unknown:{}>", defined.name)))
            }
        }
    }
}

fn decode_primitive(r: &mut BorshReader<'_>, name: &str) -> anyhow::Result<Value> {
    Ok(match name {
        "bool" => Value::Bool(r.read_bool()?),
        "u8" => json!(r.read_u8()?),
        "u16" => json!(r.read_u16()?),
        "u32" => json!(r.read_u32()?),
        "u64" => json!(r.read_u64()?.to_string()),
        "u128" => json!(r.read_u128()?.to_string()),
        "i8" => json!(r.read_i8()?),
        "i16" => json!(r.read_i16()?),
        "i32" => json!(r.read_i32()?),
        "i64" => json!(r.read_i64()?.to_string()),
        "i128" => json!(r.read_i128()?.to_string()),
        "f32" => json!(r.read_f32()?),
        "f64" => json!(r.read_f64()?),
        "string" => Value::String(r.read_string()?),
        "pubkey" | "publicKey" => Value::String(r.read_pubkey()?),
        "bytes" => {
            let len = r.read_u32()? as usize;
            Value::String(bs58::encode(r.read_bytes(len)?).into_string())
        }
        _ => { warn!(%name, "Unknown primitive"); Value::Null }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_borsh_reader_primitives() {
        let data = [1, 42, 0, 0, 0, 0, 0, 0, 0];
        let mut reader = BorshReader::new(&data);
        assert_eq!(reader.read_bool().unwrap(), true);
        assert_eq!(reader.read_u64().unwrap(), 42);
        assert!(reader.read_u8().is_err()); // EOF
    }

    #[test]
    fn test_decode_primitive() {
        // i32 = 1, then i32 = -1 (both little endian)
        let data = vec![1, 0, 0, 0, 255, 255, 255, 255]; 
        let mut r = BorshReader::new(&data);
        let val = decode_primitive(&mut r, "i32").unwrap();
        assert_eq!(val, json!(1));
        let val2 = decode_primitive(&mut r, "i32").unwrap();
        assert_eq!(val2, json!(-1));
    }
}
