//! `bootstrap.xml` and `management.xml` decoded as Artemis's
//! `XmlUtil.decode` decodes them: each case with what Artemis 2.57.0 gave,
//! as `OK`, `INVALID` and the `UnmarshalException`'s linked exception, or
//! `ERROR` and what was thrown. Properties: `artemis.home` is `D:/home`,
//! `artemis.instance` `D:/inst` and `XQ` `1`.

use saranga_config::{Dto, decode_dto};

fn decode(xml: &str) -> String {
    let dto = if xml.contains("management-context") {
        Dto::ManagementContext
    } else {
        Dto::Broker
    };
    let lookup = |n: &str| match n {
        "artemis.home" => Some("D:/home".to_owned()),
        "artemis.instance" => Some("D:/inst".to_owned()),
        "XQ" => Some("1".to_owned()),
        _ => None,
    };
    match decode_dto(dto, xml.as_bytes(), &lookup) {
        Ok(_) => "OK".to_owned(),
        Err(e)
            if !e.runtime
                && e.trace.contains(
                    "
Caused by: ",
                ) =>
        {
            let linked = e.trace.split("\nCaused by: ").nth(1).expect("a cause");
            format!("INVALID {}", linked.replace('\n', " "))
        }
        Err(e) => format!("ERROR {}", e.trace),
    }
}

/// (name, file, what Artemis gave).
const CASES: &[(&str, &str, &str)] = &[
    (
        "attrs.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security zz=\"1\" aa=\"2\" domain=\"a\"/><server configuration=\"file:x\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 92; cvc-complex-type.3.2.2: Attribute 'zz' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "bool.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"/><server configuration=\"file:x\"/><web path=\"w\" requestLog=\"x\"><binding name=\"b\" uri=\"http://h:1\" clientAuth=\"maybe\"/></web></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 139; cvc-complex-type.3.2.2: Attribute 'requestLog' is not allowed to appear in element 'web'.",
    ),
    (
        "cfgfile.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"/><server><configurationFile> x </configurationFile><configurationFile>y</configurationFile></server></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 147; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":configurationFile}'. One of '{\"http://activemq.apache.org/schema\":configurationURI}' is expected.",
    ),
    (
        "child.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"><x/></jaas-security><server configuration=\"file:x\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 97; cvc-complex-type.2.1: Element 'jaas-security' must have no character or element information item [children], because the type's content type is empty.",
    ),
    (
        "crlf.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"a\"/>\r\n  <bad a=\"b\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 15; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":bad}'. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "mgmt.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><connector connector-port=\"abc\"/></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 96; cvc-datatype-valid.1.2.1: 'abc' is not a valid value for 'integer'.",
    ),
    (
        "mgmt2.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 92; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":role-access}'. One of '{\"http://activemq.apache.org/schema\":whitelist, \"http://activemq.apache.org/schema\":allowlist, \"http://activemq.apache.org/schema\":default-access}' is expected.",
    ),
    (
        "mgmttext.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">hello<authorisation><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 151; cvc-complex-type.2.3: Element 'management-context' cannot have character [children], because the type's content type is element-only.",
    ),
    (
        "missing.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 87; cvc-complex-type.2.4.b: The content of element 'broker' is not complete. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "multi.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n  <jaas-security domain=\"a\"/>\n    <bogus/>\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 13; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":bogus}'. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "nons.xml",
        "<broker><jaas-security domain=\"a\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 9; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "ok.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"/><server configuration=\"file:x\"/></broker>",
        "OK",
    ),
    (
        "subst.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"${artemis.instance}\"/><bogus/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 104; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":bogus}'. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "text.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">junk<jaas-security domain=\"a\"/><server configuration=\"file:x\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 123; cvc-complex-type.2.3: Element 'broker' cannot have character [children], because the type's content type is element-only.",
    ),
    (
        "textin.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\">junk</jaas-security><server configuration=\"file:x\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 97; cvc-complex-type.2.1: Element 'jaas-security' must have no character or element information item [children], because the type's content type is empty.",
    ),
    (
        "big.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"/><server/><web path=\"p\" maxThreads=\"99999999999\"/></broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 127; cvc-maxInclusive-valid: Value '99999999999' is not facet-valid with respect to maxInclusive '2147483647' for type 'int'.",
    ),
    (
        "crlf2.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n<jaas-security domain=\"a\" x=\"&lt;é\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 38; cvc-complex-type.3.2.2: Attribute 'x' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "filt.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"${nope}${artemis.instance}x${artemis.home}\"/><server configuration=\"${nope}\"/><web path=\"p\" maxThreads=\"${artemis.instance}\"/></broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 200; cvc-datatype-valid.1.2.1: 'D:/inst' is not a valid value for 'integer'.",
    ),
    (
        "ioobe.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"${nopenope}-${XQ}\"/><server/></broker>\n",
        "ERROR java.lang.IndexOutOfBoundsException: Illegal start index",
    ),
    (
        "misc.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><!-- c --><?pi x?><jaas-security domain=\"a\"/><server><configurationURI>  </configurationURI></server></broker>\n",
        "OK",
    ),
    (
        "pfx.xml",
        "<b:broker xmlns:b=\"http://activemq.apache.org/schema\"><b:jaas-security domain=\"a\"/><b:server/></b:broker>\n",
        "OK",
    ),
    (
        "req.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"/><server/><web/></broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 93; cvc-complex-type.4: Attribute 'path' must appear on element 'web'.",
    ),
    (
        "rf.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"/><server/><web path=\"p\" maxThreads=\"${nope}${XQ}\"/></broker>\n",
        "ERROR java.lang.IndexOutOfBoundsException: Illegal start index",
    ),
    (
        "root1.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"/>\n",
        "OK",
    ),
    (
        "root2.xml",
        "<access xmlns=\"http://activemq.apache.org/schema\" method=\"x\"/>\n",
        "ERROR java.lang.ClassCastException: Cannot cast org.apache.activemq.artemis.dto.AccessDTO to org.apache.activemq.artemis.dto.BrokerDTO",
    ),
    (
        "text2.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n<jaas-security domain=\"a\"/><server/><web path=\"p\">é<binding/></web></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 68; cvc-complex-type.2.3: Element 'web' cannot have character [children], because the type's content type is element-only.",
    ),
    (
        "upa.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"c\"><property key=\"k\" value=\"v\"/></security-manager><server/><web path=\"a\"/><web path=\"b\"/><component/></broker>\n",
        "OK",
    ),
    (
        "ws.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"/><server/><web path=\"p\" maxThreads=\" 12 \"/><web path=\"q\" clientAuth=\" 1 \"/></broker>\n",
        "OK",
    ),
    (
        "xsi.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"a b\"><jaas-security domain=\"a\"/><server/></broker>\n",
        "OK",
    ),
    (
        "a3.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\r\rb\" zz=\"1\"/><server/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 10; cvc-complex-type.3.2.2: Attribute 'zz' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "astral.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"😀\" zz=\"1\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 86; cvc-complex-type.3.2.2: Attribute 'zz' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "bom.xml",
        "﻿<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security zz=\"1\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 74; cvc-complex-type.3.2.2: Attribute 'zz' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "c1.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r<jaas-security domain=\"a\"/>\r<bogus/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 8; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":bogus}'. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "c2.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r<jaas-security domain=\"a\"/>\r\n<bogus/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 9; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":bogus}'. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "c4.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\r<jaas-security domain=\"a\"/><bogus/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 34; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":bogus}'. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "c5.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n\r<jaas-security domain=\"a\"/><bogus/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 35; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":bogus}'. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "c7.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\rb\"/>\r <server/><bogus/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 18; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":bogus}'. One of '{\"http://activemq.apache.org/schema\":web, \"http://activemq.apache.org/schema\":component, \"http://activemq.apache.org/schema\":web}' is expected.",
    ),
    (
        "d2.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><![CDATA[x\ry]]>  <jaas-security zz=\"1\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 29; cvc-complex-type.3.2.2: Attribute 'zz' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "e1.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"></jaas-security\r><bogus/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 10; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":bogus}'. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "h3.xml",
        "<?xml version=\"1.0\"?>\r\r<broker xmlns=\"http://activemq.apache.org/schema\">\r\r<jaas-security zz=\"1\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 5; columnNumber: 22; cvc-complex-type.3.2.2: Attribute 'zz' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "m1.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><!--x\r-->  <jaas-security zz=\"1\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 28; cvc-complex-type.3.2.2: Attribute 'zz' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "p1.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><?pi a\rb?>  <jaas-security zz=\"1\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 28; cvc-complex-type.3.2.2: Attribute 'zz' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "q1.xml",
        "<!--x\ry--><broker></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 12; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "q2.xml",
        "<?pi x\ry?><broker></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 11; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "q3.xml",
        "<?xml version=\"1.0\"\r?><broker></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 11; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "q6.xml",
        "<!DOCTYPE broker [\r<!ENTITY e \"v\">\r]>\r<broker></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 4; columnNumber: 9; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "t1.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"\r zz=\"1\"/><server/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 10; cvc-complex-type.3.2.2: Attribute 'zz' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "x1.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">x\ry<jaas-security zz=\"1\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 24; cvc-complex-type.3.2.2: Attribute 'zz' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "g00073.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "OK",
    ),
    (
        "g00110.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r   <jaas-security domain=\"activemq\"/>\r   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\r   <web path=\"web\" rootRedirectLocation=\"console\">\r       <binding name=\"artemis\" uri=\"http://localhost:8161\">\r           <app name=\"console\" url=\"console\" war=\"console.war\"/>\r       </binding>\r   </web>\r</broker>\r",
        "OK",
    ),
    (
        "g00147.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"></entry>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <jaas-security method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 14; columnNumber: 52; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":jaas-security}'. One of '{\"http://activemq.apache.org/schema\":access}' is expected.",
    ),
    (
        "g00184.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         \n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      \n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 12; columnNumber: 20; cvc-complex-type.2.4.b: The content of element 'authorisation' is not complete. One of '{\"http://activemq.apache.org/schema\":role-access}' is expected.",
    ),
    (
        "g00221.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "g00258.xml",
        "<broker >\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server sniRequired=\"0x10\" configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 10; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "g00295.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"></management-context>",
        "OK",
    ),
    (
        "g00332.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<role-access configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 67; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":role-access}'. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "g00369.xml",
        "<broker xmlns=\"urn:x\">\r\n\t\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n\t</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 23; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "g00406.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"yes\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 37; cvc-datatype-valid.1.2.1: 'yes' is not a valid value for 'integer'.",
    ),
    (
        "g00443.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"> <authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "g00480.xml",
        "<broker >&amp;\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 10; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "g00517.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n\t</broker>",
        "OK",
    ),
    (
        "g00554.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <binding name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 6; columnNumber: 69; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":binding}'. One of '{\"http://activemq.apache.org/schema\":app}' is expected.",
    ),
    (
        "g00591.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 38; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":jaas-security}'. One of '{\"http://activemq.apache.org/schema\":connector, \"http://activemq.apache.org/schema\":authorisation}' is expected.",
    ),
    (
        "g00628.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\">\n   </entry></whitelist><default-access/><role-access></role-access></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 12; cvc-complex-type.2.1: Element 'entry' must have no character or element information item [children], because the type's content type is empty.",
    ),
    (
        "g00665.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/><server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 122; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":server}'. One of '{\"http://activemq.apache.org/schema\":web, \"http://activemq.apache.org/schema\":component, \"http://activemq.apache.org/schema\":web}' is expected.",
    ),
    (
        "g00702.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><role-access componentClassName=\"${}\"/><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 314; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":role-access}'. One of '{\"http://activemq.apache.org/schema\":binding, \"http://activemq.apache.org/schema\":app, \"http://activemq.apache.org/schema\":request-log}' is expected.",
    ),
    (
        "g00739.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "g00776.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"yes\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 37; cvc-datatype-valid.1.2.1: 'yes' is not a valid value for 'integer'.",
    ),
    (
        "g00813.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n   <jaas-security domain=\"activemq\"/>\r\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\r\n   <web path=\"web\" rootRedirectLocation=\"console\">\r\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\r\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\r\n       </binding>\r\n   </web>\r\n</broker>\r\n",
        "OK",
    ),
    (
        "g00850.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\r   <connector connector-port=\"1099\"/>\r   <authorisation>\r      <allowlist>\r         <entry domain=\"hawtio\"/>\r      </allowlist>\r      <default-access>\r         <access method=\"list*\" roles=\"amq\"/>\r         <access method=\"get*\" roles=\"amq\"/>\r      </default-access>\r      <role-access>\r         <match>\r            <access method=\"list*\" roles=\"amq\"/>\r            <access method=\"*\" roles=\"amq\">x</access>\r         </match>\r      </role-access>\r   </authorisation>\r</management-context>\r",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 14; columnNumber: 53; cvc-complex-type.2.1: Element 'access' must have no character or element information item [children], because the type's content type is empty.",
    ),
    (
        "g00887.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "g00924.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n  \n   <jaas-security domain=\"activemq\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:nil=\"${a\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 106; cvc-elt.3.1: Attribute 'http://www.w3.org/2001/XMLSchema-instance,nil' must not appear on element 'jaas-security', because the {nillable} property of 'jaas-security' is false.",
    ),
    (
        "g00961.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "g00998.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   \n   <management-context/><server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       \n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 25; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":management-context}'. One of '{\"http://activemq.apache.org/schema\":jaas-security, \"http://activemq.apache.org/schema\":security-manager}' is expected.",
    ),
    (
        "g01035.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "g01072.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "g01109.xml",
        "<server xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</server>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 52; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":jaas-security}'. One of '{\"http://activemq.apache.org/schema\":configurationFile, \"http://activemq.apache.org/schema\":configurationURI}' is expected.",
    ),
    (
        "g01146.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <security-manager/><access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 13; columnNumber: 32; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":security-manager}'. One of '{\"http://activemq.apache.org/schema\":access}' is expected.",
    ),
    (
        "g01220.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><![CDATA[ ]]><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "OK",
    ),
    (
        "g01257.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<!-- c -->\n<management-context xmlns=\"http://activemq.apache.org/schema\">\r\n   <connector connector-port=\"1099\"/>\r\n   <authorisation>\r\n      <allowlist>\r\n         <entry domain=\"hawtio\"/>\r\n      </allowlist>\r\n      <default-access>\r\n         <access method=\"list*\" roles=\"amq\"/>\r\n         <access method=\"get*\" roles=\"amq\"/>\r\n      </default-access>\r\n      <role-access>\r\n         <match domain=\"org.apache.activemq.artemis\">\r\n            <access method=\"list*\" roles=\"amq\"/>\r\n            <access method=\"*\" roles=\"amq\"/>\r\n         </match>\r\n      </role-access>\r\n   </authorisation>\r\n</management-context>\r\n",
        "OK",
    ),
    (
        "g01294.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\"><component/>\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"abc\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 4; columnNumber: 63; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":component}'. One of '{\"http://activemq.apache.org/schema\":binding, \"http://activemq.apache.org/schema\":app, \"http://activemq.apache.org/schema\":request-log}' is expected.",
    ),
    (
        "g01331.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<whitelist domain=\"d\" certificate-domain=\"c\"/>\r\n\t\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 48; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":whitelist}'. One of '{\"http://activemq.apache.org/schema\":jaas-security, \"http://activemq.apache.org/schema\":security-manager}' is expected.",
    ),
    (
        "g01368.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\r   <connector connector-port=\"1099\"/>\r   <authorisation>\r      <allowlist>\r         <entry domain=\"hawtio\"/>\r      </allowlist>\r      <default-access>\r         <access method=\"list*\" roles=\"amq\"/>\r         \r      </default-access>\r      <role-access>\r         <match domain=\"org.apache.activemq.artemis\">\r            <access method=\"list*\" roles=\"amq\"/>\r            <access method=\"*\" roles=\"amq\"/>\r         </match>\r      </role-access>\r   </authorisation>\r</management-context>\r",
        "OK",
    ),
    (
        "g01405.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "g01442.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"yes\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "g01479.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "g01516.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<!-- c -->\n<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "OK",
    ),
    (
        "g01553.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/><server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 114; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":server}'. One of '{\"http://activemq.apache.org/schema\":web, \"http://activemq.apache.org/schema\":component, \"http://activemq.apache.org/schema\":web}' is expected.",
    ),
    (
        "g01590.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<!-- c -->\n<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "g01664.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "g01701.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"> </entry>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 5; columnNumber: 42; cvc-complex-type.2.1: Element 'entry' must have no character or element information item [children], because the type's content type is empty.",
    ),
    (
        "g01738.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">&amp;<authorisation><whitelist><entry name=\"a${artemis.home}b\" domain=\"a\" key=\"k\"/></whitelist><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 146; cvc-complex-type.3.2.2: Attribute 'name' is not allowed to appear in element 'entry'.",
    ),
    (
        "g01775.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><component/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 157; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":component}'. One of '{\"http://activemq.apache.org/schema\":role-access}' is expected.",
    ),
    (
        "g01812.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><!-- c --><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "g01849.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "g01886.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"${XQ}${nope}\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "g01923.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "OK",
    ),
    (
        "g01960.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\">x</entry></whitelist><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 124; cvc-complex-type.2.1: Element 'entry' must have no character or element information item [children], because the type's content type is empty.",
    ),
    (
        "g01997.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/><server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 114; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":server}'. One of '{\"http://activemq.apache.org/schema\":web, \"http://activemq.apache.org/schema\":component, \"http://activemq.apache.org/schema\":web}' is expected.",
    ),
    (
        "g02034.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         \n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access roles=\"${a\">\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 11; columnNumber: 32; cvc-complex-type.3.2.2: Attribute 'roles' is not allowed to appear in element 'role-access'.",
    ),
    (
        "g02071.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 173; cvc-complex-type.2.4.d: Invalid content was found starting with element 'role-access'. No child element is expected at this point.",
    ),
    (
        "g02108.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "OK",
    ),
    (
        "g02145.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <configurationFile name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </configurationFile>\n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 5; columnNumber: 70; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":configurationFile}'. One of '{\"http://activemq.apache.org/schema\":binding, \"http://activemq.apache.org/schema\":app, \"http://activemq.apache.org/schema\":request-log}' is expected.",
    ),
    (
        "g02182.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "g02219.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "OK",
    ),
    (
        "g02256.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "g02330.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "g02367.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><security-manager/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 360; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":security-manager}'. One of '{\"http://activemq.apache.org/schema\":app, \"http://activemq.apache.org/schema\":request-log}' is expected.",
    ),
    (
        "g02404.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "g02441.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "OK",
    ),
    (
        "g02478.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<!-- c -->\n<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security componentClassName=\"${nope}\" domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 4; columnNumber: 67; cvc-complex-type.3.2.2: Attribute 'componentClassName' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "g02515.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"true\" certificate-domain=\"c\"/>\r\n\t\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 4; columnNumber: 10; cvc-complex-type.2.4.b: The content of element 'broker' is not complete. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "g02552.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "OK",
    ),
    (
        "g02589.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist uri=\"a${artemis.home}b\"><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 113; cvc-complex-type.3.2.2: Attribute 'uri' is not allowed to appear in element 'whitelist'.",
    ),
    (
        "g02626.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<!-- c -->\n<management-context xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "g02663.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "g02700.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n\t   <connector connector-port=\"1099\"/>\n\t   <authorisation>\n\t      <allowlist>\n\t         <entry domain=\"hawtio\"/>\n\t      </allowlist>\n\t      <default-access>\n\t         <access method=\"list*\" roles=\"amq\"/>\n\t         <access method=\"get*\" roles=\"amq\"/>\n\t      </default-access>\n\t      <role-access>\n\t         <match domain=\"org.apache.activemq.artemis\">\n\t            <access method=\"list*\" roles=\"amq\"/>\n\t            <access method=\"*\" roles=\"amq\"/>\n\t         </match>\n\t      </role-access>\n\t   </authorisation>\n\t</management-context>\n\t",
        "OK",
    ),
    (
        "g02737.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation rmi-registry-port=\"2147483647\"/><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 110; cvc-complex-type.3.2.2: Attribute 'rmi-registry-port' is not allowed to appear in element 'authorisation'.",
    ),
    (
        "g02774.xml",
        "<broker xmlns=\"urn:x\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><app uri=\"https://h:1\" sniRequired=\"1\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 23; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "g02811.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation>x<whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 176; cvc-complex-type.2.3: Element 'authorisation' cannot have character [children], because the type's content type is element-only.",
    ),
    (
        "g02848.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\">x</access>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 13; columnNumber: 58; cvc-complex-type.2.1: Element 'access' must have no character or element information item [children], because the type's content type is empty.",
    ),
    (
        "g02885.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "g02922.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log class-name=\"0x10\" filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 415; cvc-complex-type.3.2.2: Attribute 'class-name' is not allowed to appear in element 'request-log'.",
    ),
    (
        "g02959.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><connector/><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 90; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":connector}'. One of '{\"http://activemq.apache.org/schema\":whitelist, \"http://activemq.apache.org/schema\":allowlist, \"http://activemq.apache.org/schema\":default-access}' is expected.",
    ),
    (
        "g02996.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "hg00040.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\">\n   \n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 59; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":server}'. One of '{\"http://activemq.apache.org/schema\":jaas-security, \"http://activemq.apache.org/schema\":security-manager}' is expected.",
    ),
    (
        "hg00081.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"> </entry></whitelist><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 168; cvc-complex-type.2.1: Element 'entry' must have no character or element information item [children], because the type's content type is empty.",
    ),
    (
        "hg00122.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access value=\"éé\"/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 197; cvc-complex-type.3.2.2: Attribute 'value' is not allowed to appear in element 'role-access'.",
    ),
    (
        "hg00163.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "hg00204.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "hg00245.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<!-- c -->\n<broker xmlns=\"http://activemq.apache.org/schema\">é\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 6; columnNumber: 10; cvc-complex-type.2.3: Element 'broker' cannot have character [children], because the type's content type is element-only.",
    ),
    (
        "hg00286.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<!-- c -->\n<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "hg00327.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server/>\r\n</broker>",
        "OK",
    ),
    (
        "hg00368.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "hg00409.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "hg00450.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\" zz=\"${nope}\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 76; cvc-complex-type.3.2.2: Attribute 'zz' is not allowed to appear in element 'management-context'.",
    ),
    (
        "hg00491.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><request-log clientAuth=\"&lt;&amp;\"/><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 165; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":request-log}'. One of '{\"http://activemq.apache.org/schema\":allowlist, \"http://activemq.apache.org/schema\":default-access}' is expected.",
    ),
    (
        "hg00532.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "hg00573.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       \n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "hg00614.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\r\n   <connector connector-port=\"1099\"/>\r\n   <authorisation>\r\n      <allowlist>\r\n         <entry domain=\"hawtio\"/>\r\n      </allowlist>\r\n      <default-access>\r\n         <access method=\"list*\" roles=\"amq\"/>\r\n         <access method=\"get*\" roles=\"amq\"/>\r\n      </default-access>\r\n      <role-access>\r\n         <match domain=\"org.apache.activemq.artemis\">\r\n            <access method=\"list*\" roles=\"amq\"/>\r\n            <access method=\"*\" roles=\"amq\"/>\r\n         </match>\r\n      </role-access>\r\n   </authorisation>\r\n</management-context>\r\n",
        "OK",
    ),
    (
        "hg00655.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "hg00696.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\"><?pi x?>\n   <jaas-security domain=\"activemq\"></jaas-security>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "hg00737.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"q:int\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "OK",
    ),
    (
        "hg00778.xml",
        "<management-context xmlns=\"urn:x\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 35; cvc-elt.1.a: Cannot find the declaration of element 'management-context'.",
    ),
    (
        "hg00819.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <authorisation/><connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 20; cvc-complex-type.2.4.b: The content of element 'authorisation' is not complete. One of '{\"http://activemq.apache.org/schema\":whitelist, \"http://activemq.apache.org/schema\":allowlist, \"http://activemq.apache.org/schema\":default-access}' is expected.",
    ),
    (
        "hg00901.xml",
        "<broker >\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 10; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "hg00942.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "hg00983.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 62; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":server}'. One of '{\"http://activemq.apache.org/schema\":jaas-security, \"http://activemq.apache.org/schema\":security-manager}' is expected.",
    ),
    (
        "hg01024.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 161; cvc-complex-type.2.4.b: The content of element 'authorisation' is not complete. One of '{\"http://activemq.apache.org/schema\":role-access}' is expected.",
    ),
    (
        "hg01065.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/><jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 102; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":jaas-security}'. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "hg01106.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"> </access>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 14; columnNumber: 54; cvc-complex-type.2.1: Element 'access' must have no character or element information item [children], because the type's content type is empty.",
    ),
    (
        "hg01147.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "hg01188.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "hg01229.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "OK",
    ),
    (
        "hg01270.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "hg01311.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security key=\"é\"/>\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 75; cvc-complex-type.3.2.2: Attribute 'key' is not allowed to appear in element 'jaas-security'.",
    ),
    (
        "hg01352.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\r\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\r\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\r</broker>",
        "OK",
    ),
    (
        "hg01393.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "OK",
    ),
    (
        "hg01434.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding uri=\"http://localhost:8161\">\n           <app name=\"TRUE\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "hg01475.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security certificate-domain=\"c\"/>\r\n\t\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 41; cvc-complex-type.4: Attribute 'domain' must appear on element 'jaas-security'.",
    ),
    (
        "hg01516.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "OK",
    ),
    (
        "hg01557.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server>é<configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\">x</binding><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 228; cvc-complex-type.2.3: Element 'server' cannot have character [children], because the type's content type is element-only.",
    ),
    (
        "hg01598.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app value=\"${XQ}\" url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 69; cvc-complex-type.4: Attribute 'class-name' must appear on element 'security-manager'.",
    ),
    (
        "hg01639.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "OK",
    ),
    (
        "hg01680.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><![CDATA[ ]]><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 466; cvc-complex-type.2.4.d: Invalid content was found starting with element 'request-log'. No child element is expected at this point.",
    ),
    (
        "hg01721.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"> </property></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 159; cvc-complex-type.2.1: Element 'property' must have no character or element information item [children], because the type's content type is empty.",
    ),
    (
        "hg01762.xml",
        "<broker ><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 10; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "hg01803.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\">x</server>\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 61; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":server}'. One of '{\"http://activemq.apache.org/schema\":jaas-security, \"http://activemq.apache.org/schema\":security-manager}' is expected.",
    ),
    (
        "hg01844.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "hg01885.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\r\n\t<app domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 42; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":app}'. One of '{\"http://activemq.apache.org/schema\":jaas-security, \"http://activemq.apache.org/schema\":security-manager}' is expected.",
    ),
    (
        "hg01926.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist name=\"${artemis.instance}\">\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 4; columnNumber: 45; cvc-complex-type.3.2.2: Attribute 'name' is not allowed to appear in element 'allowlist'.",
    ),
    (
        "hg01967.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\" method=\"${a\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/><access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 76; cvc-complex-type.3.2.2: Attribute 'method' is not allowed to appear in element 'management-context'.",
    ),
    (
        "hg02008.xml",
        "<broker >\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 10; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "hg02049.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "hg02090.xml",
        "<management-context xmlns=\"urn:x\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 35; cvc-elt.1.a: Cannot find the declaration of element 'management-context'.",
    ),
    (
        "hg02131.xml",
        "<broker >\r\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\r\n</broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 10; cvc-elt.1.a: Cannot find the declaration of element 'broker'.",
    ),
    (
        "hg02172.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "hg02213.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 162; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":default-access}'. One of '{\"http://activemq.apache.org/schema\":role-access}' is expected.",
    ),
    (
        "hg02254.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\" uri=\"http://localhost:8161\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "OK",
    ),
    (
        "hg02295.xml",
        "<management-context ><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 22; cvc-elt.1.a: Cannot find the declaration of element 'management-context'.",
    ),
    (
        "hg02336.xml",
        "<management-context ><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 22; cvc-elt.1.a: Cannot find the declaration of element 'management-context'.",
    ),
    (
        "hg02377.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "OK",
    ),
    (
        "hg02418.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"activemq\"/>\n   <allowlist configuration=\"${nope}\"/><server configuration=\"file:/D:/inst/etc//broker.xml\"/>\n   <web path=\"web\" rootRedirectLocation=\"console\">\n       <binding name=\"artemis\">\n           <app name=\"console\" url=\"console\" war=\"console.war\"/>\n       </binding>\n   </web>\n</broker>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 40; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":allowlist}'. One of '{\"http://activemq.apache.org/schema\":server}' is expected.",
    ),
    (
        "hg02459.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "OK",
    ),
    (
        "hg02500.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "hg02582.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<!-- c -->\n<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "OK",
    ),
    (
        "hg02623.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"></access>\n         <access method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match>\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "hg02664.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<!-- c -->\n<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><request-log>file:x</request-log></server><web path=\"w\" maxThreads=\"${}\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><request-log filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 188; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":request-log}'. One of '{\"http://activemq.apache.org/schema\":configurationFile, \"http://activemq.apache.org/schema\":configurationURI}' is expected.",
    ),
    (
        "hg02705.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\">\r\n\t<jaas-security domain=\"d\" certificate-domain=\"c\"/>\r\n\t<server configuration=\"${artemis.instance}/etc/broker.xml\"/>\r\n</broker>",
        "OK",
    ),
    (
        "hg02746.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <access sniRequired=\"false\" method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 9; columnNumber: 65; cvc-complex-type.3.2.2: Attribute 'sniRequired' is not allowed to appear in element 'access'.",
    ),
    (
        "hg02787.xml",
        "<broker xmlns=\"http://activemq.apache.org/schema\"><security-manager class-name=\"org.x.M\"><property key=\"a\" value=\"b\"/><property key=\"c\" value=\"d\"/></security-manager><server><configurationURI>file:x</configurationURI></server><web path=\"w\" maxThreads=\"10\" clientAuth=\"true\"><binding uri=\"https://h:1\" sniRequired=\"1\"/><app url=\"u\" war=\"w\"/><jaas-security filename=\"f\" append=\"true\" retainDays=\"3\"/></web><component componentClassName=\"c\"/></broker>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 399; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":jaas-security}'. One of '{\"http://activemq.apache.org/schema\":app, \"http://activemq.apache.org/schema\":request-log}' is expected.",
    ),
    (
        "hg02828.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\" xmlns:t=\"http://activemq.apache.org/schema\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "OK",
    ),
    (
        "hg02869.xml",
        "<management-context xmlns=\"urn:x\"><authorisation><whitelist><entry domain=\"a\" key=\"k\"/></whitelist><default-access/><role-access/></authorisation></management-context>",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 35; cvc-elt.1.a: Cannot find the declaration of element 'management-context'.",
    ),
    (
        "hg02910.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n\t<connector connector-port=\"1099\"/>\n\t<authorisation>\n\t\t<allowlist>\n\t\t\t<entry domain=\"hawtio\"/>\n\t\t</allowlist>\n\t\t<default-access>\n\t\t\t<access method=\"list*\" roles=\"amq\"/>\n\t\t\t<access method=\"get*\" roles=\"amq\"/>\n\t\t</default-access>\n\t\t<role-access>\n\t\t\t<match domain=\"org.apache.activemq.artemis\">\n\t\t\t\t<access method=\"list*\" roles=\"amq\"/>\n\t\t\t\t<access method=\"*\" roles=\"amq\"/>\n\t\t\t</match>\n\t\t</role-access>\n\t</authorisation>\n</management-context>\n",
        "OK",
    ),
    (
        "hg02951.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"1099\"/>\n   <authorisation>\n      <allowlist>\n         <entry domain=\"hawtio\"/>\n      </allowlist>\n      <default-access>\n         <access method=\"list*\" roles=\"amq\"/>\n         <security-manager method=\"get*\" roles=\"amq\"/>\n      </default-access>\n      <role-access>\n         <match domain=\"org.apache.activemq.artemis\">\n            <access method=\"list*\" roles=\"amq\"/>\n            <access method=\"*\" roles=\"amq\"/>\n         </match>\n      </role-access>\n   </authorisation>\n</management-context>\n",
        "INVALID org.xml.sax.SAXParseException; lineNumber: 9; columnNumber: 55; cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"http://activemq.apache.org/schema\":security-manager}'. One of '{\"http://activemq.apache.org/schema\":access}' is expected.",
    ),
    (
        "hg02992.xml",
        "<management-context xmlns=\"http://activemq.apache.org/schema\">\n\t<connector connector-port=\"1099\"/>\n\t<authorisation>\n\t\t<allowlist>\n\t\t\t<entry domain=\"hawtio\"/>\n\t\t</allowlist>\n\t\t<default-access>\n\t\t\t<access method=\"list*\" roles=\"amq\"/>\n\t\t\t<access method=\"get*\" roles=\"amq\"/>\n\t\t</default-access>\n\t\t<role-access>\n\t\t\t<match domain=\"org.apache.activemq.artemis\">\n\t\t\t\t<access method=\"list*\" roles=\"amq\"/>\n\t\t\t\t<access method=\"*\" roles=\"amq\"/>\n\t\t\t</match>\n\t\t</role-access>\n\t</authorisation>\n</management-context>\n",
        "OK",
    ),
];

#[test]
fn decoded_as_artemis_decodes() {
    let mut wrong = Vec::new();
    for (name, xml, expected) in CASES {
        let got = decode(xml);
        if got != *expected {
            wrong.push(format!("{name}:\n  artemis: {expected}\n  saranga: {got}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
