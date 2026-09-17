pub fn push_opt(query: &mut Vec<(&str, String)>, key: &'static str, value: Option<String>) {
    if let Some(v) = value {
        query.push((key, v));
    }
}

pub fn push_opt_bool(query: &mut Vec<(&str, String)>, key: &'static str, value: Option<bool>) {
    if let Some(v) = value {
        query.push((key, v.to_string()));
    }
}

pub fn push_opt_i64(query: &mut Vec<(&str, String)>, key: &'static str, value: Option<i64>) {
    if let Some(v) = value {
        query.push((key, v.to_string()));
    }
}

pub fn push_opt_f64(query: &mut Vec<(&str, String)>, key: &'static str, value: Option<f64>) {
    if let Some(v) = value {
        query.push((key, v.to_string()));
    }
}
