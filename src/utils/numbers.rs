/// Rounds up a number `n` to the nearest multiple `m`
pub fn round_to_multiple(n: usize, m: usize) -> usize
{
    ((n + m - 1) / m) * m
}
