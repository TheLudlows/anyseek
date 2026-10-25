//! Dependency/API compatibility and literal byte-position contract.
#[cfg(feature = "zh")]
#[test]
fn jieba_adapter_uses_the_same_tantivy_api_without_long_token_filter() {
    use tantivy::tokenizer::{LowerCaser, TextAnalyzer};
    let mut tokenizer =
        tantivy_jieba::JiebaTokenizer::with_jieba(tantivy_jieba::jieba_rs::Jieba::new());
    tokenizer.set_search_mode(false);
    tokenizer.set_ordinal_position_mode(true);
    tokenizer.set_hmm(false);
    let mut analyzer = TextAnalyzer::builder(tokenizer).filter(LowerCaser).build();
    for text in ["中国 Rust，北京大学", "江苏省会是哪里"] {
        let mut stream = analyzer.token_stream(text);
        let mut tokens = Vec::new();
        while stream.advance() {
            let token = stream.token().clone();
            assert_eq!(token.position, tokens.len());
            assert_eq!(token.position_length, 1);
            assert_eq!(
                text[token.offset_from..token.offset_to].to_lowercase(),
                token.text
            );
            tokens.push(token);
        }
        assert!(!tokens.is_empty());
        eprintln!("{text}: {tokens:?}");
    }
    let long = "A".repeat(256);
    let mut stream = analyzer.token_stream(&long);
    assert!(stream.advance());
    assert_eq!(stream.token().text, long.to_lowercase());
    assert_eq!(stream.token().offset_to, long.len());
    assert!(!stream.advance());
}
