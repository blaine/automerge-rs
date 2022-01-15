#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub(super) struct ColumnSpec(u32);

impl ColumnSpec {
    pub(super) fn new(id: ColumnId, col_type: ColumnType, deflate: bool) -> Self {
        let mut raw = id.0 << 4;
        raw |= u8::from(col_type) as u32;
        if deflate {
            raw |= 0b00001000;
        } else {
            raw &= 0b11110111;
        }
        ColumnSpec(raw)
    }

    pub(super) fn col_type(&self) -> ColumnType {
        self.0.to_be_bytes()[3].into()
    }

    pub(super) fn id(&self) -> ColumnId {
        ColumnId(self.0 >> 4)
    }

    pub(super) fn deflate(&self) -> bool {
        self.0 & 0b00001000 > 0
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub(super) struct ColumnId(u32);

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub(super) enum ColumnType {
    Group,
    Actor,
    Integer,
    DeltaInteger,
    Boolean,
    String,
    ValueMetadata,
    Value,
}

impl From<u8> for ColumnType {
    fn from(v: u8) -> Self {
        let type_bits = v & 0b00000111;
        match type_bits {
            0 => Self::Group,
            1 => Self::Actor,
            2 => Self::Integer,
            3 => Self::DeltaInteger,
            4 => Self::Boolean,
            5 => Self::String,
            6 => Self::ValueMetadata,
            7 => Self::Value,
            _ => unreachable!(),
        }
    }
}

impl From<ColumnType> for u8 {
    fn from(ct: ColumnType) -> Self {
        match ct {
            ColumnType::Group => 0,
            ColumnType::Actor => 1,
            ColumnType::Integer => 2,
            ColumnType::DeltaInteger => 3,
            ColumnType::Boolean => 4,
            ColumnType::String => 5,
            ColumnType::ValueMetadata => 6,
            ColumnType::Value => 7,
        }
    }
}

impl From<u32> for ColumnSpec {
    fn from(raw: u32) -> Self {
        ColumnSpec(raw)
    }
}

impl From<ColumnSpec> for u32 {
    fn from(spec: ColumnSpec) -> Self {
        spec.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_spec_encoding() {
        struct Scenario {
            id: ColumnId,
            col_type: ColumnType,
            int_val: u32,
        }

        let scenarios = vec![
            Scenario {
                id: ColumnId(7),
                col_type: ColumnType::Group,
                int_val: 112,
            },
            Scenario {
                id: ColumnId(0),
                col_type: ColumnType::Actor,
                int_val: 1,
            },
            Scenario {
                id: ColumnId(0),
                col_type: ColumnType::Integer,
                int_val: 2,
            },
            Scenario {
                id: ColumnId(1),
                col_type: ColumnType::DeltaInteger,
                int_val: 19,
            },
            Scenario {
                id: ColumnId(3),
                col_type: ColumnType::Boolean,
                int_val: 52,
            },
            Scenario {
                id: ColumnId(1),
                col_type: ColumnType::String,
                int_val: 21,
            },
            Scenario {
                id: ColumnId(5),
                col_type: ColumnType::ValueMetadata,
                int_val: 86,
            },
            Scenario {
                id: ColumnId(5),
                col_type: ColumnType::Value,
                int_val: 87,
            },
        ];

        for (index, scenario) in scenarios.into_iter().enumerate() {
            let spec = ColumnSpec::new(scenario.id, scenario.col_type, false);

            let encoded_val = u32::from(spec);
            if encoded_val != scenario.int_val {
                panic!(
                    "Scenario {} failed encoding: expected {} but got {}",
                    index + 1,
                    scenario.int_val,
                    encoded_val
                );
            }

            if spec.col_type() != scenario.col_type {
                panic!(
                    "Scenario {} failed col type: expected {:?} but got {:?}",
                    index + 1,
                    scenario.col_type,
                    spec.col_type()
                );
            }

            if spec.deflate() {
                panic!(
                    "Scenario {} failed: spec returned true for deflate, should have been false",
                    index + 1
                );
            }

            if spec.id() != scenario.id {
                panic!(
                    "Scenario {} failed id: expected {:?} but got {:?}",
                    index + 1,
                    scenario.id,
                    spec.id()
                );
            }

            let deflated = ColumnSpec::new(scenario.id, scenario.col_type, true);

            if deflated.id() != spec.id() {
                panic!("Scenario {} failed deflate id test", index + 1);
            }

            if deflated.col_type() != spec.col_type() {
                panic!("Scenario {} failed col type test", index + 1);
            }

            if !deflated.deflate() {
                panic!(
                    "Scenario {} failed: when deflate bit set deflate returned false",
                    index + 1
                );
            }

            let expected = scenario.int_val | 0b00001000;
            if expected != u32::from(deflated) {
                panic!(
                    "Scenario {} failed deflate bit test, expected {} got {}",
                    index + 1,
                    expected,
                    u32::from(deflated)
                );
            }
        }
    }
}
