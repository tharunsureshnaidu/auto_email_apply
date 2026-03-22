use auto_email_apply::crawler::run_crawler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_crawler().await?;
    
    Ok(())
}
