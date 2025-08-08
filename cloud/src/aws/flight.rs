pub struct FlightServer {
    inner: Arc<AWSTonbo>,
}

#[async_trait]
impl FlightService for FlightServer {
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send>>;

    async fn do_get(
        &self,
        req: Request<arrow_flight::Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {

        Err(Status::unimplemented("stub"))
    }

    // stub out or implement handshake, list_flights, etc.
    async fn handshake(
        &self,
        _: Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<Response<arrow_flight::HandshakeResponse>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(
        &self,
        _: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::PutResult, Status>> + Send>>;
    async fn do_put(
        &self,
        _: Request<tonic::Streaming<arrow_flight::FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }
}