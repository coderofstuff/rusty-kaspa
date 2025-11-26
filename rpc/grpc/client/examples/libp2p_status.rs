use kaspa_grpc_client::GrpcClient;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::model::message::GetLibp2pStatusRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = std::env::var("KASPAD_GRPC_SERVER").unwrap_or_else(|_| "127.0.0.1:16510".to_string());
    let client = GrpcClient::connect(server.clone()).await?;
    let status = client.get_libp_2_p_status_call(Default::default(), GetLibp2pStatusRequest {}).await?;
    println!("server: {server}");
    println!("status: {status:?}");
    Ok(())
}
