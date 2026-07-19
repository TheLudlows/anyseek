/// DocStore 配置
#[derive(Debug, Clone)]
pub struct DocStoreConfig {
    /// 索引前缀
    pub index_prefix: String,
    /// WAL 反压阈值（字节，默认 2GB）
    pub max_wal_size: usize,
    /// L1 缓存大小（字节，默认 1GB）
    pub l1_cache_size: usize,
    /// L2 缓存大小（字节，默认 100GB）
    pub l2_cache_size: Option<usize>,
    /// L2 缓存 TTL（秒，默认 86400）
    pub l2_cache_ttl_secs: u64,
    /// 索引器间隔（ms，默认 5000）
    pub indexer_interval_ms: u64,
    /// 最小段大小（字节）
    pub min_segment_size: usize,
    /// 分块阈值（字节）
    pub chunk_threshold: usize,
}

impl Default for DocStoreConfig {
    fn default() -> Self {
        Self {
            index_prefix: "index".to_string(),
            max_wal_size: 2 * 1024 * 1024 * 1024,  // 2GB
            l1_cache_size: 1 * 1024 * 1024 * 1024,   // 1GB
            l2_cache_size: Some(100 * 1024 * 1024 * 1024), // 100GB
            l2_cache_ttl_secs: 86400,
            indexer_interval_ms: 5000,
            min_segment_size: 10 * 1024 * 1024,  // 10MB
            chunk_threshold: 4 * 1024 * 1024,    // 4MB
        }
    }
}

impl DocStoreConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_index_prefix(mut self, prefix: String) -> Self {
        self.index_prefix = prefix;
        self
    }

    pub fn with_max_wal_size(mut self, size: usize) -> Self {
        self.max_wal_size = size;
        self
    }

    pub fn with_l1_cache_size(mut self, size: usize) -> Self {
        self.l1_cache_size = size;
        self
    }

    pub fn with_l2_cache_size(mut self, size: Option<usize>) -> Self {
        self.l2_cache_size = size;
        self
    }

    pub fn with_indexer_interval(mut self, interval_ms: u64) -> Self {
        self.indexer_interval_ms = interval_ms;
        self
    }

    pub fn with_chunk_threshold(mut self, threshold: usize) -> Self {
        self.chunk_threshold = threshold;
        self
    }
}