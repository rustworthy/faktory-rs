use rand::{thread_rng, Rng};

use chrono::{DateTime, Utc};
use serde::{
    de::{Deserializer, IntoDeserializer},
    Deserialize,
};

const JOB_ID_LENGTH: usize = 16;

pub fn gen_random_jid() -> String {
    thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .map(char::from)
        .take(JOB_ID_LENGTH)
        .collect()
}

// Used to parse responses from Faktory that look like this:
// '{"jid":"f7APFzrS2RZi9eaA","state":"unknown","updated_at":""}'
pub fn parse_datetime<'de, D>(value: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<String>::deserialize(value)?.as_deref() {
        Some("") | None => Ok(None),
        Some(non_empty) => DateTime::deserialize(non_empty.into_deserializer()).map(Some),
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_jid_of_known_size_generated() {
        let jid1 = gen_random_jid();
        let jid2 = gen_random_jid();
        assert_ne!(jid1, jid2);
        println!("{}", jid1);
        assert_eq!(jid1.len(), JOB_ID_LENGTH);
        assert_eq!(jid2.len(), JOB_ID_LENGTH);
    }

    #[test]
    fn test_jids_are_unique() {
        let mut ids = HashSet::new();

        ids.insert("IYKOxEfLcwcgKaRa".to_string());
        ids.insert("IYKOxEfLcwcgKaRa".to_string());
        assert_ne!(ids.len(), 2);

        ids.clear();

        for _ in 0..1_000_000 {
            let jid = gen_random_jid();
            ids.insert(jid);
        }
        assert_eq!(ids.len(), 1_000_000);
    }
}
