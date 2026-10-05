use dbus::nonblock::stdintf::org_freedesktop_dbus::Introspectable;
use serde::Deserialize;

use super::BluetoothError;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Node {
    #[serde(rename = "@name")]
    pub name: Option<String>,
    #[serde(rename = "interface", default)]
    pub interfaces: Vec<Interface>,
    #[serde(rename = "node", default)]
    pub nodes: Vec<Node>,
}

impl Node {
    pub(crate) fn child_name(&self) -> Result<&str, BluetoothError> {
        match crate::node_name::validate(self.name.as_deref()) {
            Ok(name) => Ok(name),
            Err(crate::node_name::NameError::Missing) => Err(
                BluetoothError::RequiredPropertyMissing("introspection child node name"),
            ),
            Err(crate::node_name::NameError::Invalid) => {
                Err(BluetoothError::XmlParseError(serde_xml_rs::Error::Custom(
                    "Invalid introspection child node relative path".into(),
                )))
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Interface {
    #[serde(rename = "@name")]
    pub name: String,
    #[serde(rename = "method", default)]
    pub methods: Vec<Method>,
    #[serde(rename = "signal", default)]
    pub signals: Vec<Signal>,
    #[serde(rename = "property", default)]
    pub properties: Vec<Property>,
    #[serde(rename = "annotation", default)]
    pub annotations: Vec<Annotation>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Method {
    #[serde(rename = "@name")]
    pub name: String,
    #[serde(rename = "arg", default)]
    pub args: Vec<MethodArg>,
    #[serde(rename = "annotation", default)]
    pub annotations: Vec<Annotation>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Signal {
    #[serde(rename = "@name")]
    pub name: String,
    #[serde(rename = "arg", default)]
    pub args: Vec<SignalArg>,
    #[serde(rename = "annotation", default)]
    pub annotations: Vec<Annotation>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Property {
    #[serde(rename = "@name")]
    pub name: String,
    #[serde(rename = "@type")]
    pub dbustype: String,
    #[serde(rename = "@access")]
    pub access: Access,
    #[serde(rename = "annotation", default)]
    pub annotations: Vec<Annotation>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct MethodArg {
    #[serde(rename = "@name")]
    pub name: Option<String>,
    #[serde(rename = "@type")]
    pub dbustype: String,
    #[serde(rename = "@direction", default = "default_method_arg_direction")]
    pub direction: Direction,
    #[serde(rename = "annotation", default)]
    pub annotations: Vec<Annotation>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct SignalArg {
    #[serde(rename = "@name")]
    pub name: Option<String>,
    #[serde(rename = "@type")]
    pub dbustype: String,
    #[serde(rename = "@direction", default = "default_signal_arg_direction")]
    pub direction: Direction,
    #[serde(rename = "annotation", default)]
    pub annotations: Vec<Annotation>,
}

fn default_method_arg_direction() -> Direction {
    Direction::In
}

fn default_signal_arg_direction() -> Direction {
    Direction::Out
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Annotation {
    #[serde(rename = "@name")]
    pub name: String,
    #[serde(rename = "@value")]
    pub value: String,
}

#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq)]
pub enum Direction {
    #[serde(rename = "in")]
    In,
    #[serde(rename = "out")]
    Out,
}

#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq)]
pub enum Access {
    #[serde(rename = "readwrite")]
    ReadWrite,
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "write")]
    Write,
}

/// Extension trait to introspect D-Bus objects and parse the resulting XML into a typed structure.
pub trait IntrospectParse {
    async fn introspect_parse(&self) -> Result<Node, BluetoothError>;
}

impl<T: Introspectable + Sync> IntrospectParse for T {
    /// Introspect this object, and parse the resulting XML into a typed structure.
    async fn introspect_parse(&self) -> Result<Node, BluetoothError> {
        let introspection_xml: String = self.introspect().await?;
        let device_node: Node = serde_xml_rs::from_str(&introspection_xml)?;
        Ok(device_node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unnamed_root_is_valid_but_unnamed_child_returns_a_controlled_error() {
        let root: Node = serde_xml_rs::from_str("<node><node/></node>").unwrap();
        assert_eq!(root.name, None);
        assert!(matches!(
            root.nodes[0].child_name(),
            Err(BluetoothError::RequiredPropertyMissing(
                "introspection child node name"
            ))
        ));
    }

    #[test]
    fn malformed_child_path_components_return_xml_errors() {
        for name in ["", "service0001//child", "char-1", "desc.1", "serviceé"] {
            let root: Node =
                serde_xml_rs::from_str(&format!("<node><node name=\"{name}\"/></node>")).unwrap();
            assert!(matches!(
                root.nodes[0].child_name(),
                Err(BluetoothError::XmlParseError(_))
            ));
        }
    }

    #[test]
    fn valid_gatt_children_and_empty_lists_are_preserved() {
        let root: Node = serde_xml_rs::from_str("<node><node name=\"service0001\"/><node name=\"char0002\"/><node name=\"desc0003\"/><node name=\"other_1\"/><node name=\"service0001/child_2\"/></node>").unwrap();
        assert_eq!(
            root.nodes
                .iter()
                .map(Node::child_name)
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            [
                "service0001",
                "char0002",
                "desc0003",
                "other_1",
                "service0001/child_2"
            ]
        );
        let empty: Node = serde_xml_rs::from_str("<node/>").unwrap();
        assert!(empty.nodes.is_empty());
    }

    #[test]
    fn introspect_root() {
        let value: Node = serde_xml_rs::from_str(
            r#"<!DOCTYPE node PUBLIC "-//freedesktop//DTD D-BUS Object Introspection 1.0//EN"
            "http://www.freedesktop.org/standards/dbus/1.0/introspect.dtd">
            <node>
                <interface name="org.freedesktop.DBus.Introspectable">
                    <method name="Introspect">
                        <arg name="xml" type="s" direction="out"/>
                    </method>
                </interface>
                <interface name="org.freedesktop.DBus.ObjectManager">
                    <method name="GetManagedObjects">
                        <arg name="objects" type="a{oa{sa{sv}}}" direction="out"/>
                    </method>
                    <signal name="InterfacesAdded">
                        <arg name="object" type="o"/>
                        <arg name="interfaces" type="a{sa{sv}}"/>
                    </signal>
                    <signal name="InterfacesRemoved">
                        <arg name="object" type="o"/>
                        <arg name="interfaces" type="as"/>
                    </signal>
                </interface>
                <node name="org"/>
            </node>"#,
        )
        .unwrap();
        assert_eq!(
            value,
            Node {
                nodes: vec![Node {
                    nodes: vec![],
                    interfaces: vec![],
                    name: Some("org".to_string()),
                }],
                interfaces: vec![
                    Interface {
                        name: "org.freedesktop.DBus.Introspectable".to_string(),
                        methods: vec![Method {
                            name: "Introspect".to_string(),
                            annotations: vec![],
                            args: vec![MethodArg {
                                name: Some("xml".to_string()),
                                dbustype: "s".to_string(),
                                direction: Direction::Out,
                                annotations: vec![],
                            }],
                        }],
                        signals: vec![],
                        properties: vec![],
                        annotations: vec![],
                    },
                    Interface {
                        name: "org.freedesktop.DBus.ObjectManager".to_string(),
                        methods: vec![Method {
                            name: "GetManagedObjects".to_string(),
                            annotations: vec![],
                            args: vec![MethodArg {
                                name: Some("objects".to_string()),
                                dbustype: "a{oa{sa{sv}}}".to_string(),
                                direction: Direction::Out,
                                annotations: vec![],
                            }],
                        }],
                        signals: vec![
                            Signal {
                                name: "InterfacesAdded".to_string(),
                                annotations: vec![],
                                args: vec![
                                    SignalArg {
                                        name: Some("object".to_string()),
                                        dbustype: "o".to_string(),
                                        direction: Direction::Out,
                                        annotations: vec![],
                                    },
                                    SignalArg {
                                        name: Some("interfaces".to_string()),
                                        dbustype: "a{sa{sv}}".to_string(),
                                        direction: Direction::Out,
                                        annotations: vec![],
                                    }
                                ],
                            },
                            Signal {
                                name: "InterfacesRemoved".to_string(),
                                annotations: vec![],
                                args: vec![
                                    SignalArg {
                                        name: Some("object".to_string()),
                                        dbustype: "o".to_string(),
                                        direction: Direction::Out,
                                        annotations: vec![],
                                    },
                                    SignalArg {
                                        name: Some("interfaces".to_string()),
                                        dbustype: "as".to_string(),
                                        direction: Direction::Out,
                                        annotations: vec![],
                                    }
                                ],
                            }
                        ],
                        properties: vec![],
                        annotations: vec![],
                    },
                ],
                name: None,
            }
        )
    }
}
