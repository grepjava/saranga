//! `anyURI` as Xerces's `AnyURIDV` checks it: the value with XLink's
//! special characters escaped must make a `util.URI` relative to
//! `abc://def.ghi.jkl`, whose constructor fails only on the few things
//! ported here.

/// `AnyURIDV.getActualValue`: whether `content` is a valid `anyURI`.
pub(super) fn is_valid(content: &str) -> bool {
    content.is_empty() || parse(&encode(content))
}

/// `AnyURIDV.encode`: control characters, DEL and `` <>"{}|\^~` `` and
/// space escaped; from the first non-ASCII character on, every byte of the
/// UTF-8 that is not plain ASCII.
fn encode(s: &str) -> String {
    const ESCAPED: &[u8] = b" <>\"{}|\\^~`";
    let needs = |b: u8| b <= 0x1f || b == 0x7f || ESCAPED.contains(&b);
    let mut out = String::with_capacity(s.len() * 3);
    for &b in s.as_bytes() {
        if b >= 0x80 || needs(b) {
            out.push_str(&format!("%{b:02X}"));
        } else {
            out.push(char::from(b));
        }
    }
    out
}

fn is_hex(c: u8) -> bool {
    c.is_ascii_hexdigit()
}

fn is_mark(c: u8) -> bool {
    b"-_.!~*'()".contains(&c)
}

fn is_unreserved(c: u8) -> bool {
    c.is_ascii_alphanumeric() || is_mark(c)
}

fn is_reserved(c: u8) -> bool {
    b";/?:@&=+$,[]".contains(&c)
}

fn is_uri_char(c: u8) -> bool {
    is_unreserved(c) || is_reserved(c)
}

fn is_path_char(c: u8) -> bool {
    is_unreserved(c) || b";/:@&=+$,".contains(&c)
}

fn is_userinfo_char(c: u8) -> bool {
    is_unreserved(c) || b";:&=+$,".contains(&c)
}

fn is_scheme_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || b"+-.".contains(&c)
}

/// Whether `s[i]` starts a valid `%XX` escape.
fn escape_at(s: &[u8], i: usize) -> bool {
    i + 2 < s.len() && is_hex(s[i + 1]) && is_hex(s[i + 2])
}

/// `URI(URI base, String spec)` with a base: whether it throws
/// `MalformedURIException`.
fn parse(spec: &str) -> bool {
    let s = spec.as_bytes();
    let len = s.len();
    let mut index = 0;
    let mut scheme = false;
    if let Some(colon) = spec.find(':') {
        let before = &s[..colon];
        if colon == 0 {
            return false;
        }
        if !before.iter().any(|c| matches!(c, b'/' | b'?' | b'#')) {
            // initializeScheme and setScheme's isConformantSchemeName.
            if !before[0].is_ascii_alphabetic() || !before[1..].iter().all(|&c| is_scheme_char(c)) {
                return false;
            }
            scheme = true;
            index = colon + 1;
            if colon == len - 1 || s[colon + 1] == b'#' {
                return false;
            }
        }
    }
    if index + 1 < len && s[index] == b'/' && s[index + 1] == b'/' {
        index += 2;
        let start = index;
        while index < len && !matches!(s[index], b'/' | b'?' | b'#') {
            index += 1;
        }
        if index > start {
            if !authority(&spec[start..index]) {
                index = start - 2;
            }
        } else if index >= len {
            return false;
        }
    }
    path(s, index, scheme)
}

/// `initializePath`: the path (or opaque part), query and fragment.
fn path(s: &[u8], start: usize, scheme: bool) -> bool {
    let end = s.len();
    let mut index = start;
    let mut stop = 0u8;
    if start < end {
        if !scheme || s[start] == b'/' {
            while index < end {
                let c = s[index];
                if c == b'%' {
                    if !escape_at(s, index) {
                        return false;
                    }
                    index += 2;
                } else if !is_path_char(c) {
                    if c == b'?' || c == b'#' {
                        stop = c;
                        break;
                    }
                    return false;
                }
                index += 1;
            }
        } else {
            while index < end {
                let c = s[index];
                if c == b'?' || c == b'#' {
                    stop = c;
                    break;
                }
                if c == b'%' {
                    if !escape_at(s, index) {
                        return false;
                    }
                    index += 2;
                } else if !is_uri_char(c) {
                    return false;
                }
                index += 1;
            }
        }
    }
    if stop == b'?' {
        index += 1;
        stop = 0;
        while index < end {
            let c = s[index];
            if c == b'#' {
                stop = c;
                break;
            }
            if c == b'%' {
                if !escape_at(s, index) {
                    return false;
                }
                index += 2;
            } else if !is_uri_char(c) {
                return false;
            }
            index += 1;
        }
    }
    if stop == b'#' {
        index += 1;
        while index < end {
            let c = s[index];
            if c == b'%' {
                if !escape_at(s, index) {
                    return false;
                }
                index += 2;
            } else if !is_uri_char(c) {
                return false;
            }
            index += 1;
        }
    }
    true
}

/// `initializeAuthority`: a server-based authority, else a registry-based
/// one.
fn authority(spec: &str) -> bool {
    let s = spec.as_bytes();
    let end = s.len();
    let mut index = 0;
    let mut userinfo = None;
    if let Some(at) = spec.find('@') {
        userinfo = Some(&spec[..at]);
        index = at + 1;
    }
    let start = index;
    let mut has_port = false;
    if index < end {
        if s[start] == b'[' {
            index = spec[start..].find(']').map_or(end, |b| start + b);
            if index + 1 < end && s[index + 1] == b':' {
                index += 1;
                has_port = true;
            } else {
                index = end;
            }
        } else {
            let colon = spec.rfind(':').filter(|&c| c > start);
            index = colon.unwrap_or(end);
            has_port = index != end;
        }
    }
    let host = &spec[start..index.min(end)];
    let mut port: i64 = -1;
    if !host.is_empty() && has_port {
        let port_str = &spec[index + 1..];
        if !port_str.is_empty() {
            // Integer.parseInt, -1 and failure both made invalid.
            port = match port_str.parse::<i32>() {
                Ok(-1) | Err(_) => -2,
                Ok(p) => i64::from(p),
            };
        }
    }
    server_based(host, port, userinfo) || registry_based(s)
}

fn server_based(host: &str, port: i64, userinfo: Option<&str>) -> bool {
    if !well_formed_address(host) || !(-1..=65535).contains(&port) {
        return false;
    }
    if let Some(u) = userinfo {
        let s = u.as_bytes();
        let mut i = 0;
        while i < s.len() {
            if s[i] == b'%' {
                if !escape_at(s, i) {
                    return false;
                }
                i += 2;
            } else if !is_userinfo_char(s[i]) {
                return false;
            }
            i += 1;
        }
    }
    true
}

fn registry_based(s: &[u8]) -> bool {
    let mut i = 0;
    while i < s.len() {
        if s[i] == b'%' {
            if !escape_at(s, i) {
                return false;
            }
            i += 2;
        } else if !is_path_char(s[i]) {
            return false;
        }
        i += 1;
    }
    true
}

/// `isWellFormedAddress`: an IPv6 reference, an IPv4 address or a host
/// name.
fn well_formed_address(address: &str) -> bool {
    let s = address.as_bytes();
    let len = s.len();
    if len == 0 {
        return false;
    }
    if address.starts_with('[') {
        return ipv6_reference(address);
    }
    if address.starts_with('.') || address.starts_with('-') || address.ends_with('-') {
        return false;
    }
    let mut index = address.rfind('.');
    if address.ends_with('.') {
        index = address[..len - 1].rfind('.');
    }
    let next = index.map_or(0, |i| i + 1);
    if next < len && s[next].is_ascii_digit() {
        return ipv4_address(address);
    }
    if len > 255 {
        return false;
    }
    let mut label = 0;
    for i in 0..len {
        let c = s[i];
        if c == b'.' {
            if i == 0 || !s[i - 1].is_ascii_alphanumeric() {
                return false;
            }
            if i + 1 < len && !s[i + 1].is_ascii_alphanumeric() {
                return false;
            }
            label = 0;
        } else if !c.is_ascii_alphanumeric() && c != b'-' {
            return false;
        } else {
            label += 1;
            if label > 63 {
                return false;
            }
        }
    }
    true
}

fn ipv4_address(address: &str) -> bool {
    let s = address.as_bytes();
    let mut dots = 0;
    let mut digits = 0;
    for i in 0..s.len() {
        let c = s[i];
        if c == b'.' {
            if (i > 0 && !s[i - 1].is_ascii_digit())
                || (i + 1 < s.len() && !s[i + 1].is_ascii_digit())
            {
                return false;
            }
            digits = 0;
            dots += 1;
            if dots > 3 {
                return false;
            }
        } else if !c.is_ascii_digit() {
            return false;
        } else {
            digits += 1;
            if digits > 3 {
                return false;
            }
            if digits == 3 {
                let (first, second) = (s[i - 2], s[i - 1]);
                if !(first < b'2'
                    || (first == b'2' && (second < b'5' || (second == b'5' && c <= b'5'))))
                {
                    return false;
                }
            }
        }
    }
    dots == 3
}

fn ipv6_reference(address: &str) -> bool {
    let s = address.as_bytes();
    let len = s.len();
    let end = len.wrapping_sub(1);
    if !(len > 2 && s[0] == b'[' && s[end] == b']') {
        return false;
    }
    let mut counter = 0;
    let Some(mut index) = scan_hex_sequence(s, 1, end, &mut counter) else {
        return false;
    };
    if index == end {
        return counter == 8;
    }
    if index + 1 < end && s[index] == b':' {
        if s[index + 1] == b':' {
            counter += 1;
            if counter > 8 {
                return false;
            }
            index += 2;
            if index == end {
                return true;
            }
        } else {
            return counter == 6 && ipv4_address(&address[index + 1..end]);
        }
    } else {
        return false;
    }
    let prev = counter;
    match scan_hex_sequence(s, index, end, &mut counter) {
        Some(i) if i == end => true,
        Some(i) => ipv4_address(&address[if counter > prev { i + 1 } else { i }..end]),
        None => false,
    }
}

fn scan_hex_sequence(s: &[u8], mut index: usize, end: usize, counter: &mut i32) -> Option<usize> {
    let start = index;
    let mut digits = 0;
    while index < end {
        let c = s[index];
        if c == b':' {
            if digits > 0 {
                *counter += 1;
                if *counter > 8 {
                    return None;
                }
            }
            if digits == 0 || (index + 1 < end && s[index + 1] == b':') {
                return Some(index);
            }
            digits = 0;
        } else if !is_hex(c) {
            if c == b'.' && digits < 4 && digits > 0 && *counter <= 6 {
                let back = index as isize - digits as isize - 1;
                return Some(if back >= start as isize {
                    back as usize
                } else {
                    (back + 1) as usize
                });
            }
            return None;
        } else {
            digits += 1;
            if digits > 4 {
                return None;
            }
        }
        index += 1;
    }
    if digits > 0 {
        *counter += 1;
        if *counter <= 8 {
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::is_valid;

    #[test]
    fn uris() {
        for ok in [
            "",
            "sub/one.xml",
            "D:/tmp/a b.xml",
            "file:///D:/x.xml",
            "http://host:8080/p?q#f",
            "urn:activemq:core",
            "http://[::1]/x",
            "C:\\dir\\file.xml",
            "é.xml",
        ] {
            assert!(is_valid(ok), "{ok}");
        }
        for bad in [
            "1a:b",
            "http://",
            "a:",
            "%zz",
            "a#b#c",
            "x[y]",
            "http://[zz]/",
            ":x",
        ] {
            assert!(!is_valid(bad), "{bad}");
        }
    }
}
