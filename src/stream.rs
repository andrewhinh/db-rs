use std::collections::HashMap;
use std::io::Cursor;

use crate::Frame;

pub type ChangeEvent = (i64, String, String, Option<Vec<u8>>, i64);

pub struct TumblingWindowProcessor {
    window_ms: i64,
    allowed_lateness_ms: i64,
    counts: HashMap<i64, i64>,
    watermark: i64,
    emitted: HashMap<i64, i64>,
}

impl TumblingWindowProcessor {
    pub fn new(window_ms: i64, allowed_lateness_ms: i64) -> Self {
        Self {
            window_ms,
            allowed_lateness_ms,
            counts: HashMap::new(),
            watermark: 0,
            emitted: HashMap::new(),
        }
    }

    fn window_end(&self, ts_ms: i64) -> i64 {
        (ts_ms / self.window_ms + 1) * self.window_ms
    }

    pub fn process(&mut self, ts_ms: i64, op: &str) -> Vec<(i64, i64, bool)> {
        let mut out = Vec::new();
        if op != "set" {
            self.watermark = self.watermark.max(ts_ms);
            self.emit_closed(&mut out);
            return out;
        }

        let window_end = self.window_end(ts_ms);
        let is_late = ts_ms < self.watermark - self.allowed_lateness_ms;

        if is_late {
            let prev = self.emitted.get(&window_end).copied().unwrap_or(0);
            *self.counts.entry(window_end).or_insert(0) += 1;
            let new_count = self.counts[&window_end];
            self.emitted.insert(window_end, new_count);
            out.push((window_end, prev, true));
            out.push((window_end, new_count, true));
        } else {
            *self.counts.entry(window_end).or_insert(0) += 1;
        }

        self.watermark = self.watermark.max(ts_ms);
        self.emit_closed(&mut out);
        out
    }

    fn emit_closed(&mut self, out: &mut Vec<(i64, i64, bool)>) {
        let threshold = self.watermark - self.allowed_lateness_ms;
        let to_emit: Vec<_> = self
            .counts
            .keys()
            .copied()
            .filter(|&we| we <= threshold && !self.emitted.contains_key(&we))
            .collect();
        for we in to_emit {
            let c = self.counts[&we];
            self.emitted.insert(we, c);
            out.push((we, c, false));
        }
    }
}

pub fn parse_change(content: &[u8]) -> Option<ChangeEvent> {
    let mut cur = Cursor::new(content);
    let frame = Frame::parse(&mut cur).ok()?;
    let Frame::Array(arr) = frame else {
        return None;
    };
    if arr.len() < 5 {
        return None;
    }
    let offset = match &arr[0] {
        Frame::Integer(n) => *n,
        _ => return None,
    };
    let op = match &arr[1] {
        Frame::Simple(s) => s.clone(),
        _ => return None,
    };
    let key = match &arr[2] {
        Frame::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        _ => return None,
    };
    let value = match &arr[3] {
        Frame::Null => None,
        Frame::Bulk(b) => Some(b.to_vec()),
        _ => return None,
    };
    let ts_ms = match &arr[4] {
        Frame::Integer(n) => *n,
        _ => return None,
    };
    Some((offset, op, key, value, ts_ms))
}
