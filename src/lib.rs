/// Coinbase Pro REST API public client

// std
use std::num::NonZeroU32;
use std::fmt::Debug;
use std::time::Duration;
// external
use reqwest::{Method, Url};
use reqwest;
use governor::{
    Quota,
    RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed}
};
use anyhow;
use anyhow::Context;
use chrono::{DateTime, Utc};

/// Default Constants
pub(crate) const COINBASE_API_URL: &str = "https://api.pro.coinbase.com";
pub(crate) const DEFAULT_REQUEST_TIMEOUT: u8 = 30;
pub(crate) const DEFAULT_RATE_LIMIT: u8 = 3;
pub(crate) const DEFAULT_BURST_SIZE: u8 = 6;
pub(crate) const APP_USER_AGENT: &str = concat!(
    env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION")
);

/// Coinbase Pro public API client. Use build() method to instantiate.
#[derive(Debug)]
pub struct CoinbasePublicClient {
    api_url: &'static str,
    http_client: reqwest::Client,
    request_timeout: u8,
    rate_limiter: Option<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

/// Enum representing Coinbase's orderbook options.
#[derive(Debug, Clone)]
pub enum OBLevel {
    Level1 = 1,
    Level2 = 2,
    Level3 = 3,
}

/// Returns a tuple of length 2 that can be placed into a vector and included
/// as a request parameter.
impl OBLevel {
    fn param_tuple(&self) -> (String, String) {
        ("level".to_owned(), (self.to_owned() as u8).to_string())
    }
}

/// Enum representing Coinbase's accepted candle granularities, in seconds.
#[derive(Debug, Clone)]
pub enum Granularity {
    Minute1 = 60,
    Minute5 = 300,
    Minute15 = 900,
    Hour1 = 3600,
    Hour6 = 21600,
    Hour24 = 86400,
}

impl Granularity {
    fn param_tuple(&self) -> (String, String) {
        ("granularity".to_owned(), (self.to_owned() as u32).to_string())
    }
}

type Params = Vec<(String, String)>;

impl CoinbasePublicClient {
    /// Instantiate a new Coinbase public client using default parameters.
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Builder to construct CoinbasePublicClient instances. Parameters not passed to the builder
    /// default to the constants defined under 'Default Constants'.
    ///
    /// # Arguments
    ///
    /// * 'api_url' - API URL . Defaults to const COINBASE_API_URL (https://api.pro.coinbase.com)
    /// * 'request_timeout' - HTTP request timeout (in seconds). Defaults to const DEFAULT_REQUEST_TIMEOUT (30).
    /// * 'rate_limit' - Number of requests per second allowed. Set to zero to disable rate-limiting.
    /// Defaults to const DEFAULT_RATE_LIMIT (3).
    /// * 'burst_size' - Number of requests that can be burst when rate-limiting is enabled.
    /// Defaults to const DEFAULT_BURST_SIZE (6).
    ///
    ///  # Example
    ///             use coinbase_pro_api::CoinbasePublicClient;
    ///
    ///             let client = CoinbasePublicClient::builder()
    ///                .request_timeout(30)
    ///                .rate_limit(3)
    ///                .burst_size(6)
    ///                .api_url("https://api.pro.coinbase.com")
    ///                .build();
    pub fn builder() -> CoinbaseClientBuilder<'static> {
        CoinbaseClientBuilder::new()
    }

    /// Get list of available markets to trade.
    pub async fn get_products(&self) -> Result<String, anyhow::Error> {
        let endpoint = "/products";
        Ok(self.get_json(endpoint, None).await?)
    }

    /// Returns information about a single market
    ///
    /// # Arguments
    ///
    /// * 'product_id' - market identifier formatted as 'BASE-QUOTE', such as 'ETH-USD'.
    /// String can be lowercase or uppercase.
    pub async fn get_product(&self, product_id: &str) -> Result<String, anyhow::Error> {
        let endpoint = "/products/".to_owned() + product_id;
        Ok(self.get_json(&endpoint, None).await?)
    }

    /// Returns up to a full (level 3) orderbook from a single market.
    ///
    /// # Arguments
    ///
    /// * 'product_id' - market identifier formatted as 'BASE-QUOTE', such as 'ETH-USD'.
    /// String can be lowercase or uppercase.
    ///
    /// * 'level' - Level 1 will return the best bid and best ask.
    /// Level 2 will return the 50 best bid and ask levels, aggregated.
    /// Level 3 will return the full orderbook, unaggregated.
    pub async fn get_product_orderbook(&self, product_id: &str, level: OBLevel) -> Result<String, anyhow::Error> {
        let params: Params = vec![level.param_tuple()];
        let endpoint = format!("/products/{}/book", product_id);
        Ok(self.get_json(&endpoint, Some(params)).await?)
    }

    /// Returns snapshot about the last trade, best bid/ask and 24h volume.
    ///
    /// # Arguments
    ///
    /// * 'product_id' - market identifier formatted as 'BASE-QUOTE', such as 'ETH-USD'.
    /// String can be lowercase or uppercase.
    pub async fn get_product_ticker(&self, product_id: &str) -> Result<String, anyhow::Error> {
        let endpoint = format!("/products/{}/ticker", product_id);
        Ok(self.get_json(&endpoint, None).await?)
    }

    /// Returns a product's latest trades.
    ///
    /// # Arguments
    ///
    /// * 'product_id' - market identifier formatted as 'BASE-QUOTE', such as 'ETH-USD'.
    /// String can be lowercase or uppercase.
    /// * 'after' - optional parameter: pass in a 'Some(u64)' to parameterize a lower bound for
    /// recent trades, and exclude trades from the response that have a lower sequence.
    pub async fn get_product_trades(&self, product_id: &str, after: Option<u64>) -> Result<String, anyhow::Error> {
        let endpoint = format!("/products/{}/trades", product_id);

        let maybe_params: Option<Params> = after
            .map(|after| vec![("after".to_owned(), (after + 1).to_string())]);

        Ok(self.get_json(&endpoint, maybe_params).await?)
    }

    /// Return's a product's historic rates.
    ///
    /// # Arguments
    /// * 'product_id' - market identifier formatted as 'BASE-QUOTE', such as 'ETH-USD'.
    /// String can be lowercase or uppercase.
    /// * 'start' - optional parameter: Start DateTime<UTC>
    /// * 'end' - optional parameter: End DateTime<UTC>
    /// * 'granularity' - optional parameter: candle size in seconds
    ///
    /// The chrono crate's ['to_rfc3339'](https://docs.rs/chrono/0.4.0/chrono/struct.DateTime.html#method.to_rfc3339)
    /// method can generate the correct datetime strings.
    ///
    /// Candle schema is [timestamp, low, high, open, close, volume].
    ///
    /// If start, end, and granularity parameters are left None, Coinbase will return
    /// 300 1-minute candles. Coinbase does not publish data for periods where no trades
    /// occur. Coinbase will reject requests for more than 300 candles of any size.
    pub async fn get_product_historic_rates(
        &self,
        product_id: &str,
        start_opt: Option<DateTime<Utc>>,
        end_opt: Option<DateTime<Utc>>,
        granularity_opt: Option<Granularity>
    ) -> Result<String, anyhow::Error> {
        let endpoint = format!("/products/{}/candles", product_id);

        let mut params: Params = Vec::new();
        if start_opt.is_some() {
            params.push(("start".to_owned(), start_opt.unwrap().to_owned().to_rfc3339()))
        }
        if end_opt.is_some() {
            params.push(("end".to_owned(), end_opt.unwrap().to_owned().to_rfc3339()))
        }
        if granularity_opt.is_some() { params.push(granularity_opt.unwrap().param_tuple()); }

        let maybe_params: Option<Params> = match params.is_empty() {
            true => None,
            false => Some(params)
        };

        Ok(self.get_json(&endpoint, maybe_params).await?)
    }

    /// Returns a product's 24h stats.
    /// # Arguments
    /// * 'product_id' - market identifier formatted as 'BASE-QUOTE', such as 'ETH-USD'.
    /// String can be lowercase or uppercase.
    pub async fn get_product_24h_stats(&self, product_id: &str) -> Result<String, anyhow::Error> {
        let endpoint = format!("/products/{}/stats", product_id);
        Ok(self.get_json(&endpoint, None).await?)
    }

    /// Returns currencies supported by Coinbase.
    pub async fn get_currencies(&self) -> Result<String, anyhow::Error> {
        let endpoint = "/currencies";
        Ok(self.get_json(endpoint, None).await?)
    }


    /// Returns Coinbase's server time in both epoch and ISO format.
    pub async fn get_time(&self) -> Result<String, anyhow::Error> {
        let endpoint = "/time";
        Ok(self.get_json(endpoint, None).await?)
    }

    /// Sends get message and attempts to return json string.
    async fn get_json(&self, endpoint: &str, params: Option<Params>) -> Result<String, anyhow::Error> {
        let url_str = self.api_url.to_owned() + endpoint;

        let url = match params {
            Some(params) => {
                Url::parse_with_params(&url_str, &params)
                    .context("failed to parse url string with params")?
            },
            None => {
                Url::parse(&url_str).context("failed to parse url string")?
            }
        };

        if self.rate_limiter.is_some() {
            self.rate_limiter.as_ref().unwrap().until_ready().await;
        }

        let result= self.http_client
            .request(Method::GET, url)
            .timeout(Duration::from_secs(self.request_timeout as u64))
            .send().await.context("failure while sending request")?
            .text().await.context("failure while decoding response to text")?;

        Ok(result)
    }
}

/// Builder to construct Coinbase client instances
pub struct CoinbaseClientBuilder<'a> {
    api_url: Option<&'a str>,
    request_timeout: Option<u8>,
    rate_limit: Option<u8>,
    burst_size: Option<u8>,
}

impl CoinbaseClientBuilder<'static> {
    pub fn new() -> Self {
        Self {
            api_url: None,
            request_timeout: None,
            rate_limit: None,
            burst_size: None,
        }
    }

    pub fn api_url(self, value: &'static str) -> Self {
        Self {
            api_url: Some(value),
            ..self
        }
    }

    pub fn request_timeout(self, value: u8) -> Self {
        Self {
            request_timeout: Some(value),
            ..self
        }
    }

    pub fn rate_limit(self, value: u8) -> Self {
        Self {
            rate_limit: Some(value),
            ..self
        }
    }

    pub fn burst_size(self, value: u8) -> Self {
        Self {
            burst_size: Some(value),
            ..self
        }
    }

    pub fn build(self) -> CoinbasePublicClient {
        let rate_limit = self.rate_limit.unwrap_or(DEFAULT_RATE_LIMIT);
        let burst_size = self.burst_size.unwrap_or(DEFAULT_BURST_SIZE);

        CoinbasePublicClient {
            api_url: self.api_url.unwrap_or(COINBASE_API_URL),
            http_client: reqwest::Client::builder()
                .user_agent(APP_USER_AGENT)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            request_timeout: self.request_timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT),
            rate_limiter: {
                if rate_limit > 0 {
                    let mut quota = Quota::per_second(NonZeroU32::new(rate_limit as u32).unwrap());
                    if burst_size > 0 {
                        quota = quota.allow_burst(NonZeroU32::new(burst_size as u32).unwrap())
                    };
                    Some(RateLimiter::direct(quota))
                } else { None }
            },
        }
    }
}

impl Default for CoinbasePublicClient {
    fn default() -> Self {
        CoinbasePublicClient::new()
    }
}

impl Default for CoinbaseClientBuilder<'_> {
    fn default() -> Self {
        CoinbaseClientBuilder::new()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use chrono::prelude::*;
    use std::time::SystemTime;
    use lazy_static::lazy_static;

    fn print_type_of<T>(_: &T) {
        println!("{}", std::any::type_name::<T>());
    }

    fn now_minus_x_sec(x: u64) -> DateTime<Utc> {
        let now: DateTime<Utc> = SystemTime::now().into();
        let x = chrono::Duration::seconds(x as i64);
        now.checked_sub_signed(x).unwrap()
    }

    lazy_static! {
        static ref client: CoinbasePublicClient = CoinbasePublicClient::builder()
            .rate_limit(1)
            .burst_size(1)
            .build()
        ;
    }

    #[tokio::test]
    async fn test_time() {
        let response = client.get_time().await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_currencies() {
        let response = client.get_currencies().await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_24h_stats() {
        let response = client.get_product_24h_stats("ETH-USD").await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_candles() {
        let candles = client.get_product_historic_rates(
            "eth-usd", None, None, None
            ).await;
        assert!(candles.is_ok())
    }

    #[tokio::test]
    async fn test_trades() {
        // let client = CoinbasePublicClient::builder().build();
        let trades = client.get_product_trades("eth-usd", None).await;
        assert!(trades.is_ok())
    }

    #[tokio::test]
    async fn test_ticker() {
        let ticker = client.get_product_ticker("eth-usd").await;
        assert!(ticker.is_ok())
    }

    #[tokio::test]
    async fn test_orderbook() {
        let eth_usd_str = "eth-usd";
        let orderbook_lvl1 = client
            .get_product_orderbook(eth_usd_str, OBLevel::Level1).await;
        assert!(orderbook_lvl1.is_ok());
        let orderbook_lvl2 = client
            .get_product_orderbook(eth_usd_str, OBLevel::Level2).await;
        assert!(orderbook_lvl2.is_ok());
        let orderbook_lvl3 = client
            .get_product_orderbook(eth_usd_str, OBLevel::Level3).await;
        assert!(orderbook_lvl3.is_ok());
    }

    #[tokio::test]
    async fn get_products() {
        let products = client.get_products().await;
        assert!(products.is_ok());
    }

    #[tokio::test]
    async fn get_product() {
        let product_ids: Vec<&str> = vec![
            "ETH-USD", "btc-usd", "sol-usd"
        ];
        for product_str in product_ids {
            let result = client.get_product(product_str).await;
            assert!(result.is_ok());
        }
    }
}
