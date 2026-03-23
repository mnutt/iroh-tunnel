use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use capnp::capability::{self, Promise};
use capnp::private::capability::{
    ClientHook, ParamsHook, PipelineHook, PipelineOp, RequestHook, ResponseHook, ResultsHook,
};
use capnp::traits::{Imbue, ImbueMut};
use capnp::{Error, any_pointer, message};
use futures::TryFutureExt;
use futures::channel::oneshot;

pub(crate) type ResponseCapTableTransform = Rc<
    dyn Fn(
        Vec<Option<Box<dyn ClientHook>>>,
    ) -> Pin<
        Box<dyn Future<Output = capnp::Result<Vec<Option<Box<dyn ClientHook>>>>> + 'static>,
    >,
>;

pub(crate) type RequestCapTableTransform = Rc<
    dyn Fn(
        Vec<Option<Box<dyn ClientHook>>>,
    ) -> Pin<
        Box<dyn Future<Output = capnp::Result<Vec<Option<Box<dyn ClientHook>>>>> + 'static>,
    >,
>;

pub(crate) fn new_client_with_transforms<S>(
    server: S,
    request_transform: Option<RequestCapTableTransform>,
    response_transform: Option<ResponseCapTableTransform>,
) -> capnp::capability::Client
where
    S: capability::Server + Clone + 'static,
{
    capnp::capability::Client::new(Box::new(LocalClient::new_with_transforms(
        server,
        request_transform,
        response_transform,
    )))
}

trait ResultsDoneHook {
    fn add_ref(&self) -> Box<dyn ResultsDoneHook>;
    fn get(&self) -> capnp::Result<any_pointer::Reader<'_>>;
    fn transform_caps(
        self: Box<Self>,
        transform: ResponseCapTableTransform,
    ) -> Promise<Box<dyn ResultsDoneHook>, Error>;
}

impl Clone for Box<dyn ResultsDoneHook> {
    fn clone(&self) -> Self {
        self.add_ref()
    }
}

struct Response {
    results: Box<dyn ResultsDoneHook>,
}

impl ResponseHook for Response {
    fn get(&self) -> capnp::Result<any_pointer::Reader<'_>> {
        self.results.get()
    }
}

struct Params {
    request: message::Builder<message::HeapAllocator>,
    cap_table: Vec<Option<Box<dyn ClientHook>>>,
}

impl Params {
    fn new(
        request: message::Builder<message::HeapAllocator>,
        cap_table: Vec<Option<Box<dyn ClientHook>>>,
    ) -> Self {
        Self { request, cap_table }
    }
}

impl ParamsHook for Params {
    fn get(&self) -> capnp::Result<any_pointer::Reader<'_>> {
        let mut result: any_pointer::Reader = self.request.get_root_as_reader()?;
        result.imbue(&self.cap_table);
        Ok(result)
    }
}

struct Results {
    message: Option<message::Builder<message::HeapAllocator>>,
    cap_table: Vec<Option<Box<dyn ClientHook>>>,
    results_done_fulfiller: Option<oneshot::Sender<Box<dyn ResultsDoneHook>>>,
}

impl Results {
    fn new(fulfiller: oneshot::Sender<Box<dyn ResultsDoneHook>>) -> Self {
        Self {
            message: Some(message::Builder::new_default()),
            cap_table: Vec::new(),
            results_done_fulfiller: Some(fulfiller),
        }
    }
}

impl Drop for Results {
    fn drop(&mut self) {
        if let (Some(message), Some(fulfiller)) =
            (self.message.take(), self.results_done_fulfiller.take())
        {
            let cap_table = std::mem::take(&mut self.cap_table);
            let _ = fulfiller.send(Box::new(ResultsDone::new(message, cap_table)));
        }
    }
}

impl ResultsHook for Results {
    fn get(&mut self) -> capnp::Result<any_pointer::Builder<'_>> {
        match *self {
            Self {
                message: Some(ref mut message),
                ref mut cap_table,
                ..
            } => {
                let mut result: any_pointer::Builder = message.get_root()?;
                result.imbue_mut(cap_table);
                Ok(result)
            }
            _ => unreachable!(),
        }
    }

    fn set_pipeline(&mut self) -> capnp::Result<()> {
        Err(Error::unimplemented(
            "pipelining is not supported by the local untyped proxy".to_string(),
        ))
    }

    fn tail_call(self: Box<Self>, _request: Box<dyn RequestHook>) -> Promise<(), Error> {
        Promise::err(Error::unimplemented(
            "tail calls are not supported by the local untyped proxy".to_string(),
        ))
    }

    fn direct_tail_call(
        self: Box<Self>,
        _request: Box<dyn RequestHook>,
    ) -> (Promise<(), Error>, Box<dyn PipelineHook>) {
        (
            Promise::err(Error::unimplemented(
                "tail calls are not supported by the local untyped proxy".to_string(),
            )),
            Box::new(UnsupportedPipeline),
        )
    }

    fn allow_cancellation(&self) {}
}

struct ResultsDoneMessageInner {
    message: message::Builder<message::HeapAllocator>,
}

struct ResultsDoneInner {
    message: Rc<ResultsDoneMessageInner>,
    cap_table: Vec<Option<Box<dyn ClientHook>>>,
}

struct ResultsDone {
    inner: Rc<ResultsDoneInner>,
}

impl ResultsDone {
    fn new(
        message: message::Builder<message::HeapAllocator>,
        cap_table: Vec<Option<Box<dyn ClientHook>>>,
    ) -> Self {
        Self {
            inner: Rc::new(ResultsDoneInner {
                message: Rc::new(ResultsDoneMessageInner { message }),
                cap_table,
            }),
        }
    }
}

impl ResultsDoneHook for ResultsDone {
    fn add_ref(&self) -> Box<dyn ResultsDoneHook> {
        Box::new(Self {
            inner: self.inner.clone(),
        })
    }

    fn get(&self) -> capnp::Result<any_pointer::Reader<'_>> {
        let mut result: any_pointer::Reader = self.inner.message.message.get_root_as_reader()?;
        result.imbue(&self.inner.cap_table);
        Ok(result)
    }

    fn transform_caps(
        self: Box<Self>,
        transform: ResponseCapTableTransform,
    ) -> Promise<Box<dyn ResultsDoneHook>, Error> {
        let inner = self.inner.clone();
        Promise::from_future(async move {
            let cloned_cap_table = inner
                .cap_table
                .iter()
                .map(|entry| entry.as_ref().map(|hook| hook.add_ref()))
                .collect::<Vec<_>>();
            let transformed_cap_table = transform(cloned_cap_table).await?;
            Ok(Box::new(ResultsDone {
                inner: Rc::new(ResultsDoneInner {
                    message: inner.message.clone(),
                    cap_table: transformed_cap_table,
                }),
            }) as Box<dyn ResultsDoneHook>)
        })
    }
}

struct Request {
    message: message::Builder<message::HeapAllocator>,
    cap_table: Vec<Option<Box<dyn ClientHook>>>,
    interface_id: u64,
    method_id: u16,
    client: Box<dyn ClientHook>,
    request_cap_table_transform: Option<RequestCapTableTransform>,
    response_cap_table_transform: Option<ResponseCapTableTransform>,
}

impl Request {
    fn new(
        interface_id: u64,
        method_id: u16,
        size_hint: Option<capnp::MessageSize>,
        client: Box<dyn ClientHook>,
    ) -> Self {
        let mut allocator = message::HeapAllocator::new();
        if let Some(size_hint) = size_hint {
            allocator = allocator.first_segment_words(size_hint.word_count as u32 + 1);
        }
        Self {
            message: message::Builder::new(allocator),
            cap_table: Vec::new(),
            interface_id,
            method_id,
            client,
            request_cap_table_transform: None,
            response_cap_table_transform: None,
        }
    }

    fn with_request_cap_table_transform(
        mut self,
        request_cap_table_transform: Option<RequestCapTableTransform>,
    ) -> Self {
        self.request_cap_table_transform = request_cap_table_transform;
        self
    }

    fn with_response_cap_table_transform(
        mut self,
        response_cap_table_transform: Option<ResponseCapTableTransform>,
    ) -> Self {
        self.response_cap_table_transform = response_cap_table_transform;
        self
    }
}

impl RequestHook for Request {
    fn get(&mut self) -> any_pointer::Builder<'_> {
        let mut result: any_pointer::Builder = self.message.get_root().unwrap();
        result.imbue_mut(&mut self.cap_table);
        result
    }

    fn get_brand(&self) -> usize {
        0
    }

    fn send(self: Box<Self>) -> capability::RemotePromise<any_pointer::Owned> {
        let request = *self;
        let request_cap_table_transform = request.request_cap_table_transform.clone();
        let response_cap_table_transform = request.response_cap_table_transform.clone();
        let (results_done_fulfiller, results_done_promise) =
            oneshot::channel::<Box<dyn ResultsDoneHook>>();
        let results_done_promise = results_done_promise.map_err(|_| {
            Error::failed("local untyped proxy response channel was canceled".to_string())
        });
        let promise = Promise::from_future(async move {
            let cap_table = if let Some(transform) = request_cap_table_transform {
                transform(request.cap_table).await?
            } else {
                request.cap_table
            };
            let params = Params::new(request.message, cap_table);
            let results = Results::new(results_done_fulfiller);
            let promise = request.client.call(
                request.interface_id,
                request.method_id,
                Box::new(params),
                Box::new(results),
            );
            let ((), mut results_done_hook) =
                futures::future::try_join(promise, results_done_promise).await?;
            if let Some(transform) = response_cap_table_transform {
                results_done_hook = results_done_hook.transform_caps(transform).await?;
            }
            Ok(capability::Response::new(Box::new(Response {
                results: results_done_hook,
            })))
        });

        capability::RemotePromise {
            promise,
            pipeline: any_pointer::Pipeline::new(Box::new(UnsupportedPipeline)),
        }
    }

    fn send_streaming(self: Box<Self>) -> Promise<(), Error> {
        Promise::from_future(async {
            let _ = self.send().promise.await?;
            Ok(())
        })
    }

    fn tail_send(self: Box<Self>) -> Option<(u32, Promise<(), Error>, Box<dyn PipelineHook>)> {
        None
    }
}

#[derive(Clone)]
struct UnsupportedPipeline;

impl PipelineHook for UnsupportedPipeline {
    fn add_ref(&self) -> Box<dyn PipelineHook> {
        Box::new(self.clone())
    }

    fn get_pipelined_cap(&self, _ops: &[PipelineOp]) -> Box<dyn ClientHook> {
        Box::new(BrokenClient::new(Error::unimplemented(
            "pipelining is not supported by the local untyped proxy".to_string(),
        )))
    }
}

#[derive(Clone)]
struct BrokenClient {
    error: Error,
}

impl BrokenClient {
    fn new(error: Error) -> Self {
        Self { error }
    }
}

impl ClientHook for BrokenClient {
    fn add_ref(&self) -> Box<dyn ClientHook> {
        Box::new(self.clone())
    }

    fn new_call(
        &self,
        interface_id: u64,
        method_id: u16,
        size_hint: Option<capnp::MessageSize>,
    ) -> capability::Request<any_pointer::Owned, any_pointer::Owned> {
        capability::Request::new(Box::new(Request::new(
            interface_id,
            method_id,
            size_hint,
            self.add_ref(),
        )))
    }

    fn call(
        &self,
        _interface_id: u64,
        _method_id: u16,
        _params: Box<dyn ParamsHook>,
        _results: Box<dyn ResultsHook>,
    ) -> Promise<(), Error> {
        Promise::err(self.error.clone())
    }

    fn get_brand(&self) -> usize {
        0
    }

    fn get_ptr(&self) -> usize {
        self as *const Self as usize
    }

    fn get_resolved(&self) -> Option<Box<dyn ClientHook>> {
        None
    }

    fn when_more_resolved(&self) -> Option<Promise<Box<dyn ClientHook>, Error>> {
        None
    }

    fn when_resolved(&self) -> Promise<(), Error> {
        Promise::ok(())
    }
}

#[derive(Clone)]
struct LocalClient<S>
where
    S: capability::Server + Clone,
{
    inner: S,
    broken_error: Rc<RefCell<Option<Error>>>,
    request_cap_table_transform: Option<RequestCapTableTransform>,
    response_cap_table_transform: Option<ResponseCapTableTransform>,
}

impl<S> LocalClient<S>
where
    S: capability::Server + Clone,
{
    fn new_with_transforms(
        server: S,
        request_cap_table_transform: Option<RequestCapTableTransform>,
        response_cap_table_transform: Option<ResponseCapTableTransform>,
    ) -> Self {
        Self {
            inner: server,
            broken_error: Rc::new(RefCell::new(None)),
            request_cap_table_transform,
            response_cap_table_transform,
        }
    }
}

impl<S> ClientHook for LocalClient<S>
where
    S: capability::Server + Clone + 'static,
{
    fn add_ref(&self) -> Box<dyn ClientHook> {
        Box::new(self.clone())
    }

    fn new_call(
        &self,
        interface_id: u64,
        method_id: u16,
        size_hint: Option<capnp::MessageSize>,
    ) -> capability::Request<any_pointer::Owned, any_pointer::Owned> {
        capability::Request::new(Box::new(
            Request::new(interface_id, method_id, size_hint, self.add_ref())
                .with_request_cap_table_transform(self.request_cap_table_transform.clone())
                .with_response_cap_table_transform(self.response_cap_table_transform.clone()),
        ))
    }

    fn call(
        &self,
        interface_id: u64,
        method_id: u16,
        params: Box<dyn ParamsHook>,
        results: Box<dyn ResultsHook>,
    ) -> Promise<(), Error> {
        let streaming_error = self.broken_error.clone();
        if let Some(error) = &*streaming_error.borrow() {
            return Promise::err(error.clone());
        }
        let inner = self.inner.clone();
        Promise::from_future(async move {
            let dispatch = inner.dispatch_call(
                interface_id,
                method_id,
                capability::Params::new(params),
                capability::Results::new(results),
            );
            let result = dispatch.promise.await;
            if let (true, Err(error)) = (dispatch.is_streaming, &result) {
                *streaming_error.borrow_mut() = Some(error.clone());
            }
            result
        })
    }

    fn get_brand(&self) -> usize {
        0
    }

    fn get_ptr(&self) -> usize {
        self.inner.as_ptr()
    }

    fn get_resolved(&self) -> Option<Box<dyn ClientHook>> {
        None
    }

    fn when_more_resolved(&self) -> Option<Promise<Box<dyn ClientHook>, Error>> {
        None
    }

    fn when_resolved(&self) -> Promise<(), Error> {
        Promise::ok(())
    }
}
