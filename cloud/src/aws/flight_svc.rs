use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::{
    encode::FlightDataEncoderBuilder, error::FlightError, flight_service_server::FlightService,
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{stream::BoxStream, StreamExt};
use prost::Message;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonbo::Entry;
use tonic::{Request, Response, Status, Streaming};

use crate::{aws::AWSTonbo, gen::grpc, ScanRequest, TonboCloud};

#[derive(Clone)]
pub struct TonboFlightSvc {
    inner: Arc<AWSTonbo>,
}

impl TonboFlightSvc {
    pub fn new(tonbo: Arc<AWSTonbo>) -> Self {
        TonboFlightSvc { inner: tonbo }
    }
}

#[tonic::async_trait]
impl FlightService for TonboFlightSvc {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    // Scans Tonbo for record batches matching the scan predicates
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Unparse ticket to `ScanRequest`
        let ticket = request.into_inner().ticket;
        let scan_pb = grpc::ScanRequest::decode(ticket.as_ref())
            .map_err(|e| Status::invalid_argument(format!("Expected Ticket to be grpc::ScanRequest: {}", e)))?;
        let scan = ScanRequest::from(scan_pb);

        let (rb_tx, rb_rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(32);
        let (schema_tx, schema_rx) = oneshot::channel();

        let inner = Arc::clone(&self.inner);

        tokio::spawn(async move {
            let txn = inner.tonbo.transaction().await;

            let mut entries = match inner.read(&txn, &scan).await {
                Ok(s) => s,
                Err(e) => {
                    let _ = rb_tx
                        .send(Err(FlightError::ExternalError(e.to_string().into())))
                        .await;
                    return;
                }
            };

            // Retrieve first batch and send schema
            let first_batch = loop {
                match entries.next().await {
                    Some(Ok(Entry::RecordBatch(record_batch))) => break record_batch.record_batch().clone(),
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => {
                        let _ = rb_tx
                            .send(Err(FlightError::ExternalError(e.to_string().into())))
                            .await;
                        return;
                    }
                    None => {
                        return;
                    }
                }
            };

            let _ = schema_tx.send(first_batch.schema());

            if rb_tx.send(Ok(first_batch)).await.is_err() {
                return;
            }

            while let Some(item) = entries.next().await {
                match item {
                    Ok(Entry::RecordBatch(rb)) => {
                        if rb_tx.send(Ok(rb.record_batch().clone())).await.is_err() {
                            return;
                        }
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        let _ = rb_tx
                            .send(Err(FlightError::ExternalError(e.to_string().into())))
                            .await;
                        return;
                    }
                }
            }
        });

        let schema = schema_rx
            .await
            .map_err(|_| Status::internal("failed to get schema"))?;
        let rb_stream = ReceiverStream::new(rb_rx);

        let fd_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(rb_stream);

        let out = fd_stream.map(|res| res.map_err(|e| Status::internal(e.to_string())));
        Ok(Response::new(Box::pin(out)))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}
