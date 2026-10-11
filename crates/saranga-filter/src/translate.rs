//! `SelectorTranslator`: JMS header names in a selector become the
//! broker's (`JMSPriority` to `AMQPriority`, ...). Clients of protocols
//! other than Core send JMS selectors as they are.

/// `SelectorTranslator.convertToActiveMQFilterString`.
pub fn convert_to_activemq_filter_string(selector: &str) -> String {
    let mut s = parse(selector, "JMSDeliveryMode", "AMQDurable");
    s = parse(&s, "'PERSISTENT'", "'DURABLE'");
    s = parse(&s, "'NON_PERSISTENT'", "'NON_DURABLE'");
    s = parse(&s, "JMSPriority", "AMQPriority");
    s = parse(&s, "JMSTimestamp", "AMQTimestamp");
    s = parse(&s, "JMSMessageID", "AMQUserID");
    s = parse(&s, "JMSExpiration", "AMQExpiration");
    parse(&s, "JMSXGroupID", "AMQGroupID")
}

/// `Character.isJavaIdentifierPart`, for the characters selectors use.
fn identifier_part(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// `SelectorTranslator.parse`: replaces `m` where it is a whole
/// identifier outside quotes (or anywhere when `m` itself is quoted).
/// After a partial match fails the scan resumes at the next character, as
/// in Artemis.
pub fn parse(input: &str, m: &str, replace: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let pattern: Vec<char> = m.chars().collect();
    let replace_in_quotes = pattern.first() == Some(&'\'');
    let mut in_quote = false;
    let mut pos = 0;
    let mut positions = Vec::new();
    for (i, &c) in chars.iter().enumerate() {
        if c == '\'' {
            in_quote = !in_quote;
        }
        if (!in_quote || replace_in_quotes) && c == pattern[pos] {
            pos += 1;
            if pos == pattern.len() {
                let after = chars.get(i + 1).is_some_and(|&c| identifier_part(c));
                let before = (i + 1)
                    .checked_sub(pattern.len() + 1)
                    .is_some_and(|b| identifier_part(chars[b]));
                if !after && !before {
                    positions.push(i + 1 - pattern.len());
                }
                pos = 0;
            }
        } else {
            pos = 0;
        }
    }
    if positions.is_empty() {
        return input.to_owned();
    }
    let mut out = String::new();
    let mut start = 0;
    for p in positions {
        out.extend(&chars[start..p]);
        out.push_str(replace);
        start = p + pattern.len();
    }
    out.extend(&chars[start..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_headers_outside_quotes() {
        assert_eq!(
            convert_to_activemq_filter_string("JMSPriority > 5 AND JMSDeliveryMode = 'PERSISTENT'"),
            "AMQPriority > 5 AND AMQDurable = 'DURABLE'"
        );
        assert_eq!(
            convert_to_activemq_filter_string("x = 'JMSPriority' OR myJMSPriority = 1"),
            "x = 'JMSPriority' OR myJMSPriority = 1"
        );
    }
}
