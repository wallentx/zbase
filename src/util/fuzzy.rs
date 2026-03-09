#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FuzzyMatch {
    pub score: i32,
    pub ranges: Vec<(usize, usize)>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CandidateChar {
    lower: char,
    start: usize,
    end: usize,
}

#[derive(Clone, Debug)]
pub struct PreparedFuzzyQuery {
    pub(crate) chars: Vec<char>,
    pub(crate) char_len: usize,
    pub(crate) char_mask: u128,
    pub(crate) words: Vec<Vec<char>>,
    pub(crate) is_multi_word: bool,
}

#[derive(Clone, Debug)]
pub struct PreparedFuzzyCandidate {
    pub(crate) chars: Vec<CandidateChar>,
    pub(crate) char_len: usize,
    pub(crate) char_mask: u128,
}

const CONTIGUOUS_FRAGMENT_BONUS: i32 = 12;
const EXTRA_FRAGMENT_PENALTY: i32 = 8;

pub fn fuzzy_match(pattern: &str, candidate: &str) -> Option<FuzzyMatch> {
    let prepared_query = prepare_fuzzy_query(pattern);
    let Some(prepared_query) = prepared_query else {
        return Some(FuzzyMatch {
            score: 0,
            ranges: Vec::new(),
        });
    };
    let prepared_candidate = prepare_fuzzy_candidate(candidate);
    fuzzy_match_prepared(&prepared_query, &prepared_candidate)
}

pub fn prepare_fuzzy_query(pattern: &str) -> Option<PreparedFuzzyQuery> {
    let trimmed = pattern.trim();
    if trimmed.is_empty() {
        return None;
    }
    let chars = trimmed
        .chars()
        .map(|ch| ch.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let char_mask = chars
        .iter()
        .fold(0u128, |mask, ch| mask | char_mask_bit(*ch));
    let words: Vec<Vec<char>> = trimmed
        .split_whitespace()
        .map(|w| w.chars().map(|ch| ch.to_ascii_lowercase()).collect())
        .collect();
    let is_multi_word = words.len() > 1;
    Some(PreparedFuzzyQuery {
        char_len: chars.len(),
        chars,
        char_mask,
        words,
        is_multi_word,
    })
}

pub fn prepare_fuzzy_candidate(candidate: &str) -> PreparedFuzzyCandidate {
    let chars = candidate
        .char_indices()
        .map(|(start, ch)| CandidateChar {
            lower: ch.to_ascii_lowercase(),
            start,
            end: start + ch.len_utf8(),
        })
        .collect::<Vec<_>>();
    let char_mask = chars
        .iter()
        .fold(0u128, |mask, ch| mask | char_mask_bit(ch.lower));
    PreparedFuzzyCandidate {
        char_len: chars.len(),
        chars,
        char_mask,
    }
}

pub fn fuzzy_match_prepared(
    query: &PreparedFuzzyQuery,
    candidate: &PreparedFuzzyCandidate,
) -> Option<FuzzyMatch> {
    if query.chars.is_empty() || candidate.chars.is_empty() {
        return None;
    }
    if query.is_multi_word {
        if let Some(word_match) = word_match_prepared(query, candidate) {
            return Some(word_match);
        }
        return None;
    }
    fuzzy_match_prepared_inner(&query.chars, candidate)
}

fn fuzzy_match_prepared_inner(
    pattern: &[char],
    candidate: &PreparedFuzzyCandidate,
) -> Option<FuzzyMatch> {
    let earliest = greedy_match_indices(pattern, &candidate.chars)?;
    let tightened = tighten_match_indices(pattern, &candidate.chars, &earliest);

    let earliest_ranges = to_ranges(&candidate.chars, &earliest);
    let earliest_score = score_match(&candidate.chars, &earliest, &earliest_ranges);

    let tightened_ranges = to_ranges(&candidate.chars, &tightened);
    let tightened_score = score_match(&candidate.chars, &tightened, &tightened_ranges);

    let use_tightened = tightened_score > earliest_score
        || (tightened_score == earliest_score && tightened_ranges.len() < earliest_ranges.len());
    if use_tightened {
        Some(FuzzyMatch {
            score: tightened_score,
            ranges: tightened_ranges,
        })
    } else {
        Some(FuzzyMatch {
            score: earliest_score,
            ranges: earliest_ranges,
        })
    }
}

fn word_match_prepared(
    query: &PreparedFuzzyQuery,
    candidate: &PreparedFuzzyCandidate,
) -> Option<FuzzyMatch> {
    if candidate.chars.is_empty() {
        return None;
    }
    let candidate_lower: String = candidate.chars.iter().map(|ch| ch.lower).collect();

    let mut all_ranges: Vec<(usize, usize)> = Vec::new();
    let mut score = 0i32;
    let mut words_matched = 0usize;

    for word in &query.words {
        if word.is_empty() {
            continue;
        }
        let word_str: String = word.iter().collect();
        let mut best_pos: Option<usize> = None;
        let mut best_boundary = false;
        let mut search_start = 0usize;
        while let Some(rel_pos) = candidate_lower[search_start..].find(&word_str) {
            let pos = search_start + rel_pos;
            let at_boundary = is_boundary_byte_pos(&candidate.chars, pos);
            if best_pos.is_none() || (at_boundary && !best_boundary) {
                best_pos = Some(pos);
                best_boundary = at_boundary;
                if at_boundary {
                    break;
                }
            }
            search_start = pos + 1;
            if search_start >= candidate_lower.len() {
                break;
            }
        }
        let pos = best_pos?;
        let byte_start = candidate.chars[pos].start;
        let byte_end = candidate.chars[pos + word.len() - 1].end;
        all_ranges.push((byte_start, byte_end));

        score += word.len() as i32 * 2;
        if best_boundary {
            score += 10;
        }
        words_matched += 1;
    }

    if words_matched == 0 {
        return None;
    }

    score += CONTIGUOUS_FRAGMENT_BONUS * words_matched as i32;
    score -= candidate.char_len as i32;

    all_ranges.sort_by_key(|r| r.0);
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for range in all_ranges {
        match merged.last_mut() {
            Some(last) if last.1 >= range.0 => {
                last.1 = last.1.max(range.1);
            }
            _ => merged.push(range),
        }
    }

    Some(FuzzyMatch {
        score,
        ranges: merged,
    })
}

fn is_boundary_byte_pos(chars: &[CandidateChar], char_index: usize) -> bool {
    if char_index == 0 {
        return true;
    }
    let previous = chars[char_index - 1].lower;
    previous.is_whitespace()
        || matches!(
            previous,
            '-' | '_' | '#' | '>' | '.' | '/' | '\\' | ':' | ',' | '(' | ')'
        )
}

fn greedy_match_indices(pattern: &[char], candidate: &[CandidateChar]) -> Option<Vec<usize>> {
    let mut cursor = 0usize;
    let mut matched = Vec::with_capacity(pattern.len());
    for needle in pattern {
        let mut found = None;
        for idx in cursor..candidate.len() {
            if candidate[idx].lower == *needle {
                found = Some(idx);
                break;
            }
        }
        let idx = found?;
        matched.push(idx);
        cursor = idx + 1;
    }
    Some(matched)
}

fn tighten_match_indices(
    pattern: &[char],
    candidate: &[CandidateChar],
    earliest: &[usize],
) -> Vec<usize> {
    if pattern.is_empty() {
        return Vec::new();
    }

    let mut tightened = vec![0usize; pattern.len()];
    let mut right_bound = candidate.len().saturating_sub(1);
    for pat_idx in (0..pattern.len()).rev() {
        let needle = pattern[pat_idx];
        let left_bound = if pat_idx == 0 {
            0
        } else {
            earliest[pat_idx - 1].saturating_add(1)
        };
        let mut found = None;
        if left_bound <= right_bound {
            for idx in (left_bound..=right_bound).rev() {
                if candidate[idx].lower == needle {
                    found = Some(idx);
                    break;
                }
            }
        }
        let idx = found.unwrap_or(earliest[pat_idx]);
        tightened[pat_idx] = idx;
        if idx == 0 {
            break;
        }
        right_bound = idx - 1;
    }
    tightened
}

fn score_match(
    candidate_chars: &[CandidateChar],
    matched_indices: &[usize],
    ranges: &[(usize, usize)],
) -> i32 {
    let mut score = 0i32;
    for (i, idx) in matched_indices.iter().copied().enumerate() {
        score += 1;
        if i == 0 && idx == 0 {
            score += 3;
        }
        if is_boundary(candidate_chars, idx) {
            score += 10;
        }
        if let Some(previous) = i
            .checked_sub(1)
            .and_then(|p| matched_indices.get(p).copied())
        {
            if idx == previous + 1 {
                score += 5;
            } else if idx > previous + 1 {
                score -= (idx - previous - 1) as i32;
            }
        }
    }
    match ranges.len() {
        0 => {}
        1 => score += CONTIGUOUS_FRAGMENT_BONUS,
        fragments => score -= (fragments as i32 - 1) * EXTRA_FRAGMENT_PENALTY,
    }
    score - candidate_chars.len() as i32
}

fn is_boundary(chars: &[CandidateChar], index: usize) -> bool {
    if index == 0 {
        return true;
    }
    let previous = chars[index - 1].lower;
    previous.is_whitespace()
        || matches!(
            previous,
            '-' | '_' | '#' | '>' | '.' | '/' | '\\' | ':' | ',' | '(' | ')'
        )
}

fn char_mask_bit(ch: char) -> u128 {
    let lowered = ch.to_ascii_lowercase();
    let slot = if lowered.is_ascii_lowercase() {
        (lowered as u8 - b'a') as u32
    } else if lowered.is_ascii_digit() {
        26 + (lowered as u8 - b'0') as u32
    } else if lowered.is_ascii_whitespace() {
        36
    } else {
        37 + (lowered as u32 % 91)
    };
    1u128 << slot.min(127)
}

fn to_ranges(chars: &[CandidateChar], matched_indices: &[usize]) -> Vec<(usize, usize)> {
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for idx in matched_indices {
        let part = (chars[*idx].start, chars[*idx].end);
        match ranges.last_mut() {
            Some(last) if last.1 == part.0 => last.1 = part.1,
            _ => ranges.push(part),
        }
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzzy_matches_chge_for_chia_general() {
        let result = fuzzy_match("chge", "chia general").expect("expected match");
        assert!(!result.ranges.is_empty());
    }

    #[test]
    fn fuzzy_matches_gech_for_group_dm_names() {
        let result = fuzzy_match("gech", "Gene Hoffman, Chris Foudy").expect("expected match");
        assert!(!result.ranges.is_empty());
    }

    #[test]
    fn fuzzy_non_match_returns_none() {
        assert!(fuzzy_match("zzzz", "chia general").is_none());
    }

    #[test]
    fn fuzzy_prefers_fewer_fragments_for_same_letters() {
        let contiguous = fuzzy_match("alert", "#alert").expect("expected contiguous match");
        let fragmented = fuzzy_match("alert", "#a_l_e_r_t").expect("expected fragmented match");
        assert!(
            contiguous.score > fragmented.score,
            "contiguous matches should outrank fragmented matches"
        );
        assert_eq!(contiguous.ranges.len(), 1);
        assert!(fragmented.ranges.len() > 1);
    }

    #[test]
    fn fuzzy_chooses_compact_late_match_over_greedy_early_fragments() {
        let result = fuzzy_match("alert", "a_l_e_r_t #alert").expect("expected match");
        assert_eq!(
            result.ranges.len(),
            1,
            "expected compact fragment instead of greedy split match"
        );
    }

    #[test]
    fn prepared_match_equivalent_for_basic_case() {
        let query = prepare_fuzzy_query("chge").expect("prepared query");
        let candidate = prepare_fuzzy_candidate("chia general");
        let matched = fuzzy_match_prepared(&query, &candidate).expect("prepared match");
        assert!(!matched.ranges.is_empty());
    }

    #[test]
    fn word_match_gene_hoffman_finds_dm() {
        let result =
            fuzzy_match("gene hoffman", "Gene Hoffman, Chris Foudy").expect("expected match");
        assert!(!result.ranges.is_empty());
        assert!(result.score > 0, "multi-word match should score positively");
    }

    #[test]
    fn word_match_gene_chr_finds_dm() {
        let result = fuzzy_match("gene chr", "Gene Hoffman, Chris Foudy").expect("expected match");
        assert!(!result.ranges.is_empty());
    }

    #[test]
    fn word_match_gene_chris_finds_dm() {
        let result =
            fuzzy_match("gene chris", "Gene Hoffman, Chris Foudy").expect("expected match");
        assert!(!result.ranges.is_empty());
    }

    #[test]
    fn word_match_rejects_scattered_letters() {
        let result = fuzzy_match("dang it", "Design and Engineering Team");
        assert!(
            result.is_none(),
            "multi-word query should not match scattered letters: {:?}",
            result,
        );
    }

    #[test]
    fn word_match_prefers_boundary_alignment() {
        let result =
            fuzzy_match("gene hoffman", "Gene Hoffman, Chris Foudy").expect("expected match");
        assert!(
            result.score > 20,
            "boundary-aligned word match should score highly"
        );
    }

    #[test]
    fn single_word_still_uses_fuzzy() {
        let result = fuzzy_match("chge", "chia general").expect("expected fuzzy match");
        assert!(!result.ranges.is_empty());
    }

    #[test]
    fn word_match_ge_ch_finds_dm() {
        let result = fuzzy_match("ge ch", "Gene Hoffman, Chris Foudy").expect("expected match");
        assert!(!result.ranges.is_empty());
    }
}
