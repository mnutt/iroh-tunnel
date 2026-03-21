pub mod stream_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/stream_capnp.rs"));
}
pub mod persistent_capnp {
    pub mod persistent {
        pub trait Server<SturdyRef, Owner>: 'static
        where
            SturdyRef: ::capnp::traits::Owned,
            Owner: ::capnp::traits::Owned,
        {
        }

        pub struct ServerDispatch<_T, SturdyRef, Owner> {
            pub server: ::capnp::capability::Rc<_T>,
            _phantom: ::core::marker::PhantomData<(SturdyRef, Owner)>,
        }

        impl<_T, SturdyRef, Owner> ServerDispatch<_T, SturdyRef, Owner>
        where
            SturdyRef: ::capnp::traits::Owned,
            Owner: ::capnp::traits::Owned,
        {
            pub fn dispatch_call_internal(
                _server: ::capnp::capability::Rc<_T>,
                _method_id: u16,
                _params: ::capnp::capability::Params<::capnp::any_pointer::Owned>,
                _results: ::capnp::capability::Results<::capnp::any_pointer::Owned>,
            ) -> ::capnp::capability::DispatchCallResult {
                ::capnp::capability::DispatchCallResult::new(
                    ::capnp::capability::Promise::err(::capnp::Error::unimplemented(
                        "persistent capability server dispatch not implemented".to_string(),
                    )),
                    false,
                )
            }
        }
    }
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
pub mod supervisor_capnp {
    include!(concat!(env!("OUT_DIR"), "/sandstorm/supervisor_capnp.rs"));
}
pub mod powerbox_capnp {
    include!(concat!(env!("OUT_DIR"), "/sandstorm/powerbox_capnp.rs"));
}
pub mod ip_capnp {
    include!(concat!(env!("OUT_DIR"), "/sandstorm/ip_capnp.rs"));
}
pub mod api_session_capnp {
    include!(concat!(env!("OUT_DIR"), "/sandstorm/api_session_capnp.rs"));
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
