use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn test_server_accepts_connections() {
    // Start server in background
    let server_handle = tokio::spawn(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        
        // Return port to test
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                // Just accept and drop for now
                drop(stream);
            }
        });
        
        port
    });
    
    // Get the port
    let port = server_handle.await.unwrap();
    
    // Try to connect
    let result = timeout(
        Duration::from_secs(1),
        TcpStream::connect(format!("127.0.0.1:{}", port))
    ).await;
    
    assert!(result.is_ok(), "Should be able to connect to server");
}

#[tokio::test]
async fn test_multiple_connections() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    // Accept connections in background
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
        }
    });
    
    // Connect multiple times
    for i in 0..5 {
        let result = timeout(
            Duration::from_secs(1),
            TcpStream::connect(format!("127.0.0.1:{}", port))
        ).await;
        
        assert!(result.is_ok(), "Connection {} should succeed", i);
    }
}