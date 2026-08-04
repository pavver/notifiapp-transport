pub fn validate_protocol_string(s: &str) {
    for b in s.bytes() {
        let valid = (b >= b'a' && b <= b'z')
            || (b >= b'A' && b <= b'Z')
            || (b >= b'0' && b <= b'9')
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
