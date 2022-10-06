/// Coinbase Pro REST API public client using Async Rust

// std
use std::num::NonZeroU32;
use std::fmt::Debug;
use std::convert::From;
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
use serde_json;
use serde_json::{Value};
use anyhow;
use anyhow::Context;

/// Default Constants
const COINBASE_API_URL: &str = "https://api.pro.coinbase.com";
const DEFAULT_REQUEST_TIMEOUT: u8 = 30;
const DEFAULT_RATE_LIMIT: u8 = 3;
const DEFAULT_BURST_SIZE: u8 = 6;
const APP_USER_AGENT: &str = concat!(
    env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION")
);

/// Coinbase Pro public API client object.
///
///     url: API URL. Defaults to Coinbase Pro API.
///     client: Persistent HTTP connection object.
///     request_timeout: HTTP request timeout (in seconds).
///     rate_limit: Number of requests per second allowed. Set to zero to disable rate-limiting.
///     burst_size: Number of requests that can be burst when rate-limiting is enabled.
pub struct CoinbasePublicClient {
    api_url: &'static str,
    http_client: reqwest::Client,
    request_timeout: u8,
    rate_limiter: Option<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

/// Enum representing Coinbase's orderbook options.
///
///     Level1: returns best bid and best ask
///     Level2: returns 50 best levels, aggregated, for both bids and asks
///     Level3: returns full orderbook, unaggregated
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
    /// default to the constants defined under 'Default Constants'. Parameters include:
    ///
    ///     api_url: &str                               Defaults to COINBASE_API_URL
    ///     request_timeout: u8                         Defaults to DEFAULT_REQUEST_TIMEOUT
    ///     rate_limit: u8                              Defaults to DEFAULT_RATE_LIMIT
    ///     burst_size: u8                              Defaults to DEFAULT_BURST_SIZE
    ///     return_type: DeserializeInto                Defaults to DEFAULT_RETURN_TYPE
    ///
    ///     Pass parameters to the builder as such:
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
    ///
    /// Data is deserialized into an instance of serde_json's ['Value'] struct.
    pub async fn get_products(&self) -> Result<Value, anyhow::Error> {
        let endpoint = "/products";
        let json = self.get_json(endpoint, None).await?;
        self.deserialize(&json).await
    }

    /// Returns information about a single market.
    ///
    /// Data is deserialized into an instance of serde_json's ['Value'] struct.
    pub async fn get_product(&self, product_id: &str) -> Result<Value, anyhow::Error> {
        let endpoint = "/products/".to_owned() + product_id;
        let json = self.get_json(&endpoint, None).await?;
        self.deserialize(&json).await
    }

    /// Returns up to a full (level 3) orderbook from a single market.
    ///
    ///     Level 1 will return the best bid and best ask.
    ///     Level 2 will return the 50 best bid and ask levels, aggregated.
    ///     Level 3 will return the full orderbook, unaggregated.
    ///
    /// Data is deserialized into an instance of serde_json's ['Value'] struct.
    pub async fn get_product_orderbook(&self, product_id: &str, level: OBLevel) -> Result<Value, anyhow::Error> {
        let params: Params = vec![level.param_tuple()];
        let endpoint = format!("/products/{}/book", product_id);
        let json = self.get_json(&endpoint, Some(params)).await?;
        self.deserialize(&json).await
    }

    /// Returns snapshot about the last trade, best bid/ask and 24h volume.
    ///
    /// Data is deserialized into an instance of serde_json's ['Value'] struct.
    pub async fn get_product_ticker(&self, product_id: &str) -> Result<Value, anyhow::Error> {
        let endpoint = format!("/products/{}/ticker", product_id);
        let json = self.get_json(&endpoint, None).await?;
        self.deserialize(&json).await
    }

    /// Returns a product's latest trades.
    ///
    /// Data is deserialized into an instance of serde_json's ['Value'] struct.
    pub async fn get_product_trades(&self, product_id: &str, trade_id: Option<u32>) -> Result<Value, anyhow::Error> {
        let endpoint = format!("/products/{}/trades", product_id);

        let maybe_params: Option<Params> = trade_id
            .map(|trade_id| vec![("after".to_owned(), (trade_id + 1).to_string())]);

        let json = self.get_json(&endpoint, maybe_params).await?;

        self.deserialize(&json).await
    }

    /// Return's a product's historic rates.
    ///
    ///     start: Start time in ISO 8601
    ///     end: End time in ISO 8601
    ///     granularity: candle size in seconds
    ///
    /// Candle schema is [timestamp, low, high, open, close, volume].
    ///
    /// If start, end, and granularity parameters are left None, Coinbase will return
    /// 300 1-minute candles. Coinbase does not publish data for periods where no trades
    /// occur. Coinbase will reject requests for more than 300 candles of any size.
    ///
    /// Data is deserialized into an instance of serde_json's ['Value'] struct.
    pub async fn get_product_historic_rates(&self, product_id: &str, start_opt: Option<&str>,
                        end_opt: Option<&str>, granularity_opt: Option<Granularity>) -> Result<Value, anyhow::Error> {
        let endpoint = format!("/products/{}/candles", product_id);

        let mut params: Params = Vec::new();
        if start_opt.is_some() { params.push(("start".to_owned(), start_opt.unwrap().to_owned())) }
        if end_opt.is_some() { params.push(("end".to_owned(), end_opt.unwrap().to_owned())) }
        if granularity_opt.is_some() { params.push(granularity_opt.unwrap().param_tuple()); }

        let maybe_params: Option<Params> = match params.len() {
            0 => None,
            _ => Some(params)
        };

        let json = self.get_json(&endpoint, maybe_params).await?;

        self.deserialize(&json).await

    }

    /// Returns a product's 24h stats.
    ///
    /// Data is deserialized into an instance of serde_json's ['Value'] struct.
    pub async fn get_product_24h_stats(&self, product_id: &str) -> Result<Value, anyhow::Error> {
        let endpoint = format!("/products/{}/stats", product_id);
        let json = self.get_json(&endpoint, None).await?;
        self.deserialize(&json).await
    }

    /// Returns currencies supported by Coinbase.
    ///
    /// Data is deserialized into an instance of serde_json's ['Value'] struct.
    pub async fn get_currencies(&self) -> Result<Value, anyhow::Error> {
        let endpoint = "/currencies";
        let json = self.get_json(endpoint, None).await?;
        self.deserialize(&json).await
    }


    /// Returns Coinbase's server time in both epoch and ISO format.
    ///
    /// Data is deserialized into an instance of serde_json's ['Value'] struct.
    pub async fn get_time(&self) -> Result<Value, anyhow::Error> {
        let endpoint = "/time";
        let json = self.get_json(endpoint, None).await?;
        self.deserialize(&json).await
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

    /// Sends get message to an endpoint and attempts to load response into serde's Value enum.
    ///
    /// Slightly more overhead but very likely to stay compatible with any future changes
    /// to Coinbase's API, since Value represents any valid JSON response.
    async fn deserialize(&self, json: &str) -> Result<Value, anyhow::Error> {
        serde_json::from_str(json).map_err(anyhow::Error::from)
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


/// It is not recommended to run these tests in parallel, as that may result in
/// excessive polling, leading to reduced or blocked API access.
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Error;
    use barter_integration::model::{Instrument, InstrumentKind, Symbol};
    use chrono::prelude::*;
    use std::time::SystemTime;
    use serde_json::Value;

    fn print_type_of<T>(_: &T) {
        println!("{}", std::any::type_name::<T>());
    }

    fn short_str(instrument: &Instrument) -> String {
        format!("{:?}-{:?}", instrument.base, instrument.quote)
    }

    fn iso8601_minus_x_sec(x: u64) -> String {
        let now: DateTime<Utc> = SystemTime::now().into();
        let x = chrono::Duration::seconds(x as i64);
        let now_minus_x = now.checked_sub_signed(x).unwrap();
        now_minus_x.to_rfc3339()
    }

    #[tokio::test]
    async fn test_time() {
        let client = CoinbasePublicClient::new();
        let response = client.get_time().await.unwrap();
        assert!(response.get("epoch").is_some());
        assert!(response.get("iso").is_some());
    }

    // #[tokio::test]
    // async fn test_currencies() {
    //     let client = CoinbasePublicClient::new();
    //     let response = client.get_currencies().await.unwrap();
    //     assert!(response.as_array().unwrap().len()>100);
    // }
    //
    // #[tokio::test]
    // async fn test_24h_stats() {
    //     let client = CoinbasePublicClient::new();
    //     let response = client.get_product_24h_stats("eth-usd").await.unwrap();
    //     assert!(response.get("last").is_some());
    //     assert!(response.get("open").is_some());
    //     assert!(response.get("high").is_some());
    //     assert!(response.get("low").is_some());
    //     assert!(response.get("volume").is_some());
    // }
    //
    // #[tokio::test]
    // async fn test_candles() {
    //     let client = CoinbasePublicClient::new();
    //     let response = client.get_product_historic_rates(
    //         "eth-usd", None, None, None
    //         ).await.unwrap();
    //     assert_eq!(response.as_array().unwrap().len(), 300);
    //     let first_candle_timestamp = response.pointer("/0/0").unwrap().as_i64().unwrap();
    //     let last_candle_timestamp = response.pointer("/299/0").unwrap().as_i64().unwrap();
    //     let difference = first_candle_timestamp - last_candle_timestamp;
    //     assert!(difference/60 > 290 && difference/60 < 310);
    //
    //     let hours_ago_24 = iso8601_minus_x_sec(86400);
    //     let hours_ago_4 = iso8601_minus_x_sec(14400);
    //     let response = client.get_product_historic_rates(
    //         "eth-usd", Some(&hours_ago_24),
    //         Some(&hours_ago_4), Some(Granularity::Hour6)
    //     ).await.unwrap();
    //     assert_eq!(response.as_array().unwrap().len(), 4);
    //     let first_candle_timestamp = response.pointer("/0/0").unwrap().as_i64().unwrap();
    //     let last_candle_timestamp = response.pointer("/3/0").unwrap().as_i64().unwrap();
    //     let difference = first_candle_timestamp - last_candle_timestamp;
    //     assert_eq!(difference/3600, 18);
    // }
    //
    // #[tokio::test]
    // async fn test_trades() {
    //     let client = CoinbasePublicClient::builder().build();
    //     let trades = client.get_product_trades("eth-usd", None).await;
    //     println!("{:?}", trades);
    // }
    //
    // #[tokio::test]
    // async fn test_ticker() {
    //     let client = CoinbasePublicClient::builder().build();
    //     let ticker = client.get_product_ticker("eth-usd").await;
    //     println!("{:?}", ticker);
    // }
    //
    // #[tokio::test]
    // async fn test_orderbook() {
    //     let client = CoinbasePublicClient::new();
    //     let eth_usd_str = "eth-usd";
    //     let orderbook_lvl1 = client
    //         .get_product_orderbook(eth_usd_str, OBLevel::Level1).await;
    //     assert!(orderbook_lvl1.is_ok());
    //     // if orderbook_lvl1.is_ok() { println!("{:?}", orderbook_lvl1.unwrap()) }
    //     let orderbook_lvl2 = client
    //         .get_product_orderbook(eth_usd_str, OBLevel::Level2).await;
    //     assert!(orderbook_lvl2.is_ok());
    //     // if orderbook_lvl2.is_ok() { println!("{:?}", orderbook_lvl2.unwrap()) }
    //     let orderbook_lvl3 = client
    //         .get_product_orderbook(eth_usd_str, OBLevel::Level3).await;
    //     assert!(orderbook_lvl3.is_ok());
    //     // if orderbook_lvl3.is_ok() { println!("{:?}", orderbook_lvl3.unwrap()) }
    // }
    //
    // #[tokio::test]
    // async fn test_tuple() {
    //     let ob_enum_1 = OBLevel::Level1;
    //     println!("{:?}", ob_enum_1.param_tuple());
    // }
    //
    // #[tokio::test]
    // async fn test_builder() {
    //     // nonsense parameters are okay for this test
    //     let api_url = "https://api.google.com";
    //     let rate_limit = 10;
    //     let request_timeout = 5;
    //     let burst_size = 3;
    //
    //     let client = CoinbasePublicClient::builder()
    //         .rate_limit(rate_limit)
    //         .request_timeout(request_timeout)
    //         .burst_size(burst_size)
    //         .api_url(api_url)
    //         .build();
    //     assert_eq!(client.rate_limit, rate_limit);
    //     assert_eq!(client.request_timeout, request_timeout);
    //     assert_eq!(client.burst_size, burst_size);
    //     assert_eq!(client.api_url, api_url);
    //     assert!(client.rate_limiter.is_some());
    //
    //     let client = CoinbasePublicClient::builder()
    //         .rate_limit(0)
    //         .build();
    //     assert_eq!(client.rate_limit, 0);
    //     assert_eq!(client.request_timeout, DEFAULT_REQUEST_TIMEOUT);
    //     assert_eq!(client.burst_size, DEFAULT_BURST_SIZE);
    //     assert_eq!(client.api_url, COINBASE_API_URL);
    //     assert!(client.rate_limiter.is_none());
    //
    //     let client = CoinbasePublicClient::new();
    //     assert_eq!(client.rate_limit, DEFAULT_RATE_LIMIT);
    //     assert_eq!(client.request_timeout, DEFAULT_REQUEST_TIMEOUT);
    //     assert_eq!(client.burst_size, DEFAULT_BURST_SIZE);
    //     assert_eq!(client.api_url, COINBASE_API_URL);
    //     assert!(client.rate_limiter.is_some());
    // }
    //
    // #[tokio::test]
    // async fn get_products() {
    //     let client = CoinbasePublicClient::new();
    //     let products = client.get_products().await;
    //     // println!("{:?}", products);
    //     assert!(products.is_ok());
    // }
    //
    // #[tokio::test]
    // async fn get_product() {
    //     let client = CoinbasePublicClient::new();
    //     let spot_instruments: Vec<Instrument> = vec![
    //         Instrument::new("eth", "usd", InstrumentKind::Spot),
    //         Instrument::new("btc", "usd", InstrumentKind::Spot),
    //         Instrument::new("doge", "usd", InstrumentKind::Spot),
    //     ];
    //     let products_short_str: Vec<String> = spot_instruments
    //         .iter().map(|instrument| short_str(instrument)).collect();
    //
    //     for product_str in products_short_str {
    //         let result = client.get_product(&product_str).await;
    //         assert!(result.is_ok());
    //     }
    // }
}
