//! Helpers for building [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/)
//! (REdis Serialization Protocol) replies.
//!
//! Keeping every reply behind a small, well-named function means the wire
//! format lives in exactly one place. Command handlers describe *what* they
//! want to return (an integer, a bulk string, an array) and never touch raw
//! `\r\n` framing themselves.

/// A `+<value>\r\n` simple string, e.g. `+OK\r\n`.
pub fn simple_string(value: &str) -> String {
    format!("+{value}\r\n")
}

/// A `-<message>\r\n` error. The message is expected to already carry its
/// error code, e.g. `"ERR unknown command"` or `"WRONGTYPE ..."`.
pub fn error(message: &str) -> String {
    format!("-{message}\r\n")
}

/// A `:<value>\r\n` integer reply.
pub fn integer(value: i64) -> String {
    format!(":{value}\r\n")
}

/// A `$<len>\r\n<value>\r\n` bulk string. `len` is the byte length, as RESP
/// requires.
pub fn bulk_string(value: &str) -> String {
    format!("${}\r\n{value}\r\n", value.len())
}

/// The `$-1\r\n` null bulk string, used to signal "no value".
pub fn null() -> String {
    "$-1\r\n".to_string()
}

/// A `*<len>\r\n...` array whose elements are each encoded as bulk strings.
pub fn array(items: &[String]) -> String {
    let mut out = format!("*{}\r\n", items.len());
    for item in items {
        out.push_str(&bulk_string(item));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_scalar_replies() {
        assert_eq!(simple_string("OK"), "+OK\r\n");
        assert_eq!(error("ERR nope"), "-ERR nope\r\n");
        assert_eq!(integer(1), ":1\r\n");
        assert_eq!(integer(0), ":0\r\n");
        assert_eq!(bulk_string("hi"), "$2\r\nhi\r\n");
        assert_eq!(null(), "$-1\r\n");
    }

    #[test]
    fn encodes_arrays() {
        assert_eq!(
            array(&["a".to_string(), "bb".to_string()]),
            "*2\r\n$1\r\na\r\n$2\r\nbb\r\n"
        );
        assert_eq!(array(&[]), "*0\r\n");
    }
}
