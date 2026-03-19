pub mod stream_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/stream_capnp.rs"));
}
pub mod util_capnp {
    include!(concat!(env!("OUT_DIR"), "/sandstorm/util_capnp.rs"));
}
pub mod identity_capnp {
    include!(concat!(env!("OUT_DIR"), "/sandstorm/identity_capnp.rs"));
}
pub mod activity_capnp {
    include!(concat!(env!("OUT_DIR"), "/sandstorm/activity_capnp.rs"));
}
pub mod powerbox_capnp {
    include!(concat!(env!("OUT_DIR"), "/sandstorm/powerbox_capnp.rs"));
}
pub mod grain_capnp {
    include!(concat!(env!("OUT_DIR"), "/sandstorm/grain_capnp.rs"));
}
pub mod web_session_capnp {
    include!(concat!(env!("OUT_DIR"), "/sandstorm/web_session_capnp.rs"));
}
pub mod sandstorm_http_bridge_capnp {
    include!(concat!(env!("OUT_DIR"), "/sandstorm/sandstorm_http_bridge_capnp.rs"));
}
