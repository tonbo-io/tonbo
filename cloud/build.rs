fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()  
    .build_server(true)  
    .out_dir("src/gen")  
    .compile_protos(  
        &["src/proto/cloud.proto"],  
        &["src/proto"],  
    )?;
    Ok(())
}