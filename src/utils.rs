pub fn validate_protocol_string(s: &str) {
    for b in s.bytes() {
        let valid = b.is_ascii_lowercase()
            || b.is_ascii_uppercase()
            || b.is_ascii_digit()
            || b == b'.'
            || b == b'-'
            || b == b'_';
        if !valid {
            panic!(
                "Invalid character in protocol string (only alphanumeric, '.', '-', '_' are allowed): {}",
                s
            );
        }
    }
}
