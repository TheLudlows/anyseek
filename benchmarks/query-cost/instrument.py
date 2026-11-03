"""Optional phase timers: apply only to a disposable profiling checkout."""
from pathlib import Path
import sys
root=Path(sys.argv[1]).resolve()
def edit(path, pairs):
 p=root/path;s=p.read_text()
 for a,b in pairs:
  assert a in s,a
  s=s.replace(a,b,1)
 p.write_text(s)
clock='let probe_start = std::time::Instant::now();'
mark=lambda name: f'eprintln!("PROBE {name} {{}}", probe_start.elapsed().as_nanos());'
edit('anyseek/src/record/lexical/runtime_query.rs',[
 ('    let root = index.read().await;\n    let shards',f'    {clock}\n    let root = index.read().await;\n    let shards'),
 ('    let corpus = super::scoring::CorpusView::select(',f'    {mark("capture")}\n    {clock}\n    let corpus = super::scoring::CorpusView::select('),
 ('    let names = config',f'    {mark("select")}\n    {clock}\n    let names = config'),
 ('    let indexed_fields = config',f'    {mark("analysis")}\n    let indexed_fields = config'),
 ])
edit('anyseek/src/record/lexical/scoring.rs',[
 ('        let statistics = QueryStatistics::build(&references,',f'        {clock}\n        let statistics = QueryStatistics::build(&references,'),
 ('        let mut count = 0usize;',f'        {mark("bm25_statistics")}\n        {clock}\n        let mut count = 0usize;'),
 ('        Ok(count)\n    }\n\n    #[cfg(test)]',f'        {mark("score_collect")}\n        Ok(count)\n    }}\n\n    #[cfg(test)]'),
 ])
edit('anyseek/src/record/query.rs',[
 ('        let mut hits = Vec::with_capacity(candidates.len());',f'        {clock}\n        let mut hits = Vec::with_capacity(candidates.len());'),
 ('        barrier.deliver(|| ())?;',f'        {mark("revalidate_copy")}\n        {clock}\n        let statistics = inner.statistics();\n        {mark("return_statistics")}\n        barrier.deliver(|| ())?;'),
 ('            statistics: inner.statistics(),','            statistics,'),
 ])
