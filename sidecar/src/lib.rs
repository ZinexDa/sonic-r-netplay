pub mod loopback;
pub mod punch;
pub mod runner;

pub fn init_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

